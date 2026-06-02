#!/usr/bin/env python3
"""
WebSocket-based BTC price feeder with optional Chainlink Data Streams support.

Behavior:
- If environment variable `CHAINLINK_WS_URL` is set, this module will try to
    connect to that URL and send an optional subscription message (see
    `CHAINLINK_SUBSCRIBE_MSG`). Authentication headers can be supplied via
    `CHAINLINK_API_KEY`.
- If `CHAINLINK_WS_URL` is not set, falls back to CoinCap public websocket
    (the previous behavior).

Configurable environment variables:
- `CHAINLINK_WS_URL` : websocket endpoint (e.g. from Chainlink Data Streams docs)
- `CHAINLINK_API_KEY`: API key to include in handshake headers (optional)
- `CHAINLINK_SUBSCRIBE_MSG`: JSON string to send after connect to subscribe
- `CHAINLINK_PRICE_PATH`: dotted path in incoming JSON to the numeric price

See Data Streams WebSocket docs: https://docs.chain.link/data-streams/reference/data-streams-api/interface-ws
"""
import asyncio
import json
import os
import time
import contextlib
import inspect
import websockets
from logger_config import setup_logger

logger = setup_logger('chainlink')

# Cached latest prices from the websocket feed
current_chainlink_prices = {}


async def subscribe_chainlink():
    """Subscribe to CoinCap websocket for bitcoin price and keep cache updated.

    Uses: wss://ws.coincap.io/prices?assets=bitcoin
    The message payload is a small JSON like {"bitcoin": "12345.67"}.
    """
    # Prefer explicit Chainlink WS if provided; otherwise use CoinCap fallback
    url = os.environ.get("CHAINLINK_WS_URL") or "wss://ws.coincap.io/prices?assets=bitcoin"
    api_key = os.environ.get("CHAINLINK_API_KEY")
    subscribe_msg = os.environ.get("CHAINLINK_SUBSCRIBE_MSG")
    price_path = os.environ.get("CHAINLINK_PRICE_PATH")  # dotted path to price in incoming JSON
    retry_count = 0
    max_retries = 999999

    while retry_count < max_retries:
        try:
            # Build headers for handshake. If using Chainlink, include API key headers.
            extra_headers = {
                "User-Agent": "polymarket-l2-collector/1.0"
            }
            if api_key:
                # Some providers expect Authorization Bearer, others x-api-key; include both.
                extra_headers["Authorization"] = f"Bearer {api_key}"
                extra_headers["x-api-key"] = api_key

            # If a fallback (CoinCap) URL is used, add Origin for CoinCap.
            if "coincap.io" in url:
                extra_headers.setdefault("Origin", "https://coincap.io")

            # websockets.connect implementations differ between versions; try supported kwarg
            def _connect_cm(u, hdrs):
                try:
                    # try extra_headers first (newer websockets)
                    return websockets.connect(u, extra_headers=hdrs)
                except TypeError:
                    try:
                        # older versions may accept 'headers'
                        return websockets.connect(u, headers=hdrs)
                    except TypeError:
                        # fallback: no headers
                        return websockets.connect(u)

            async with _connect_cm(url, extra_headers) as ws:
                retry_count = 0
                logger.info("Connected to CoinCap chainlink proxy websocket")

                async def _ping_loop():
                    try:
                        while True:
                            await asyncio.sleep(15)
                            try:
                                await ws.ping()
                            except Exception:
                                return
                    except asyncio.CancelledError:
                        return

                ping_task = asyncio.create_task(_ping_loop())

                # If provided, send a subscription message (some Chainlink streams require it)
                if subscribe_msg:
                    try:
                        await ws.send(subscribe_msg)
                        logger.info('Sent subscribe message to Chainlink WS')
                    except Exception as e:
                        logger.warning(f'Failed to send subscribe message: {e}')

                try:
                    while True:
                        msg = await ws.recv()
                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            # ignore non-json frames
                            continue

                        # Extract price according to configured path, or try sensible defaults
                        p = None
                        if price_path:
                            # dotted path e.g. data.price
                            node = data
                            for part in price_path.split('.'):
                                if isinstance(node, dict) and part in node:
                                    node = node[part]
                                else:
                                    node = None
                                    break
                            if node is not None:
                                try:
                                    p = float(node)
                                except Exception:
                                    p = None
                        else:
                            # common Chainlink stream payloads may include numeric fields; try a few
                            # Try top-level 'price', 'value', or asset name keys
                            for key in ('price', 'value', 'result', 'data'):
                                val = data.get(key)
                                if isinstance(val, (int, float)):
                                    p = float(val)
                                    break
                                if isinstance(val, str):
                                    try:
                                        p = float(val)
                                        break
                                    except Exception:
                                        pass
                            # fallback: scan for first numeric leaf
                            if p is None:
                                def find_numeric(v):
                                    if isinstance(v, (int, float)):
                                        return float(v)
                                    if isinstance(v, str):
                                        try:
                                            return float(v)
                                        except Exception:
                                            return None
                                    if isinstance(v, dict):
                                        for kk in v.values():
                                            r = find_numeric(kk)
                                            if r is not None:
                                                return r
                                    if isinstance(v, list):
                                        for el in v:
                                            r = find_numeric(el)
                                            if r is not None:
                                                return r
                                    return None
                                p = find_numeric(data)

                        if p is None:
                            continue

                        current_chainlink_prices['BTC'] = {
                            'mid': p,
                            'time': time.time()
                        }
                except websockets.ConnectionClosed as e:
                    code = getattr(e, "code", None)
                    reason = getattr(e, "reason", "") or ""
                    logger.warning(f'Chainlink websocket closed, code={code} reason={reason}; reconnecting')
                    # If server explicitly rejects with Unauthorized, back off longer (avoid tight reconnect loops)
                    if "Unauthorized" in str(reason) or "unauthorized" in str(reason).lower():
                        backoff = 300  # 5 minutes
                        logger.error(f'Chainlink websocket handshake unauthorized; backing off {backoff}s before retry')
                        await asyncio.sleep(backoff)
                        continue
                except Exception as e:
                    logger.error(f'Chainlink receive error: {e}')
                finally:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await ping_task
        except Exception as e:
            retry_count += 1
            wait_time = min(5 * retry_count, 60)
            logger.warning(f'Chainlink ws connect failed: {e}, retry in {wait_time}s')
            await asyncio.sleep(wait_time)


def get_chainlink_price_usd(coin: str) -> float:
    """Return latest cached USD price for `coin` (only `btc` supported).

    Returns 0.0 if no price is available.
    """
    if not coin or coin.lower() != 'btc':
        return 0.0
    entry = current_chainlink_prices.get('BTC')
    if not entry:
        return 0.0
    return float(entry.get('mid', 0.0))


async def main():
    await subscribe_chainlink()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('chainlink_price stopped')

