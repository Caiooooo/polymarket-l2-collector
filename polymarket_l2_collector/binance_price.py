"""
Binance bookTicker WebSocket — midprice for BTC/ETH.

Provides ``current_prices`` dict (symbol → {bid, ask, mid, spread, time})
and the ``subscribe_book_ticker()`` coroutine.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime

import websockets

from .logger_config import get_logger

logger = get_logger("binance")

BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
SYMBOLS = ["btcusdt", "ethusdt"]
PING_INTERVAL = 20
PING_TIMEOUT = 10
MAX_SIZE = 2**19  # 512 KB

current_prices: dict[str, dict] = {}
last_message_time: float = 0.0


async def subscribe_book_ticker() -> None:
    """Subscribe to Binance bookTicker streams with auto-reconnect."""
    streams = "/".join(f"{s}@bookTicker" for s in SYMBOLS)
    ws_url = f"{BINANCE_WS_URL}?streams={streams}"
    retry_count = 0

    while True:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=5,
                max_size=MAX_SIZE,
            ) as ws:
                retry_count = 0
                logger.info("Binance WS connected")
                keepalive = asyncio.create_task(_keepalive_pong(ws))

                try:
                    while True:
                        raw = await ws.recv()
                        global last_message_time
                        last_message_time = time.time()

                        data = json.loads(raw).get("data", {})
                        symbol = data.get("s", "")
                        best_bid = float(data.get("b", 0))
                        best_ask = float(data.get("a", 0))

                        current_prices[symbol] = {
                            "bid": best_bid,
                            "ask": best_ask,
                            "mid": (best_bid + best_ask) / 2,
                            "spread": best_ask - best_bid,
                            "time": datetime.now().strftime("%H:%M:%S.%f")[:-3],
                        }
                except websockets.exceptions.ConnectionClosed:
                    logger.warning("Binance WS disconnected, reconnecting …")
                except Exception as exc:
                    logger.error("Binance receive error: %s", exc)
                finally:
                    keepalive.cancel()
                    try:
                        await keepalive
                    except asyncio.CancelledError:
                        pass

        except asyncio.CancelledError:
            logger.info("Binance task cancelled")
            raise
        except Exception as exc:
            retry_count += 1
            delay = min(5 * (2 ** min(retry_count - 1, 4)), 120)
            delay += random.uniform(0, delay * 0.3)
            logger.warning("Binance WS error, retry in %.1fs (attempt %d): %s", delay, retry_count, exc)
            await asyncio.sleep(delay)


async def _keepalive_pong(ws) -> None:
    """Send unsolicited PONG every 30s as proxy/NAT keepalive."""
    try:
        while True:
            await asyncio.sleep(30)
            await ws.pong()
    except websockets.exceptions.ConnectionClosed:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.debug("Keepalive PONG error: %s", exc)


if __name__ == "__main__":
    asyncio.run(subscribe_book_ticker())
