#!/usr/bin/env python3
"""
Polymarket WebSocket 5分钟市场数据获取脚本
"""
import asyncio
import json
import websockets
import time
from binance_price import current_prices
from chainlink_price import get_chainlink_price_usd
from file_cache import save_trades, save_book
from asset_utils import get_assets
from logger_config import setup_logger

logger = setup_logger('poly_5m')

# Polymarket WebSocket 端点
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
INTERVAL = "5m"
INTERVAL_SECONDS = 5 * 60
# 不提前预连接，保持单个活动 websocket


def create_asset_mapping(assets):
    """创建 asset_id 到币种的映射"""
    asset_to_coin = {}
    for coin, interval_data in assets.items():
        for interval, asset_data in interval_data.items():
            if isinstance(asset_data, dict):
                # 仅映射 up 方向的 asset_id（只保存 up）
                asset_to_coin[asset_data.get("up", "")] = coin.upper() + "_up_" + interval
    return asset_to_coin


def get_asset_price(coin):
    """从 current_prices 获取对应币种的价格"""
    # 对于 BTC 使用 Chainlink（通过 chainlink_price 模块）；其余使用 Binance mid price
    if coin.lower() == "btc":
        price = get_chainlink_price_usd(coin)
        return price or 0.0

    symbol = f"{coin}USDT"
    if symbol in current_prices:
        return current_prices[symbol].get("mid", 0.0)
    return 0.0


def format_orderbook_data(poly_data, asset_to_coin, window_open_ts=None):
    """将 poly_ws 数据格式化为示例格式"""
    formatted_data = []
    for item in poly_data:
        asset_id = item.get("asset_id", "")
        coin = asset_to_coin.get(asset_id, "")
        if not coin:
            continue
        asset_price = get_asset_price(coin.split("_")[0])
        timestamp = item.get("timestamp", str(int(time.time() * 1000)))
        local_timestamp = str(int(time.time() * 1000))
        formatted_item = {
            "bids": item.get("bids", []),
            "asks": item.get("asks", []),
            "local_timestamp": local_timestamp,
            "timestamp": timestamp,
            "asset_price": asset_price,
            "window_open_ts": window_open_ts
        }
        formatted_data.append(formatted_item)
    return formatted_data


def format_trade_data(poly_data, asset_to_coin, window_open_ts=None):
    """将 poly_ws 交易数据格式化为示例格式"""
    formatted_data = []
    for item in poly_data:
        asset_id = item.get("asset_id", "")
        coin = asset_to_coin.get(asset_id, "")
        if not coin:
            continue
        asset_price = get_asset_price(coin.split("_")[0])
        timestamp = item.get("timestamp", str(int(time.time() * 1000)))
        local_timestamp = str(int(time.time() * 1000))
        formatted_item = {
            "price": item.get("price", "0"),
            "size": item.get("size", "0"),
            "side": item.get("side", "").lower(),
            "local_timestamp": local_timestamp,
            "timestamp": timestamp,
            "asset_price": asset_price,
            "window_open_ts": window_open_ts
        }
        formatted_data.append(formatted_item)
    return formatted_data


def get_market_window_timestamp(data):
    """根据消息自带时间戳计算所属 5m 窗口的起始时间戳。"""
    if not data:
        return int(time.time() // INTERVAL_SECONDS) * INTERVAL_SECONDS

    raw_timestamp = data[0].get("timestamp") or data[0].get("local_timestamp")
    if raw_timestamp is None:
        return int(time.time() // INTERVAL_SECONDS) * INTERVAL_SECONDS

    timestamp = int(raw_timestamp)
    if timestamp > 1_000_000_000_000:
        timestamp //= 1000

    return (timestamp // INTERVAL_SECONDS) * INTERVAL_SECONDS


def get_next_market_timestamp(now_timestamp):
    """计算下一场 5m 市场的起始时间戳。"""
    return ((now_timestamp // INTERVAL_SECONDS) + 1) * INTERVAL_SECONDS


def save_book_data(data, asset_to_coin, window_open_ts=None):
    """保存格式化的订单簿数据"""
    if not data:
        return
    now_opening_market = window_open_ts or get_market_window_timestamp(data)
    formatted_data = format_orderbook_data(
        data, asset_to_coin, window_open_ts=now_opening_market)
    if not formatted_data:
        return
    asset_id = data[0].get("asset_id", "")
    coin = asset_to_coin.get(asset_id, "")
    if not coin or "_" not in coin:
        return
    up_or_down = coin.split("_")[1]
    coin_name = coin.split("_")[0]
    file_path = f"data/{INTERVAL}/{coin_name.lower()}/orderbooks/{now_opening_market}{up_or_down}.parquet"
    save_book(formatted_data, file_path)


def save_trade_data(data, asset_to_coin, window_open_ts=None):
    """保存格式化的交易数据"""
    if not data:
        return
    now_opening_market = window_open_ts or get_market_window_timestamp(data)
    formatted_data = format_trade_data(
        data, asset_to_coin, window_open_ts=now_opening_market)
    if not formatted_data:
        return
    asset_id = data[0].get("asset_id", "")
    coin = asset_to_coin.get(asset_id, "")
    if not coin or "_" not in coin:
        return
    up_or_down = coin.split("_")[1]
    coin_name = coin.split("_")[0]
    file_path = f"data/{INTERVAL}/{coin_name.lower()}/trades/{now_opening_market}{up_or_down}.parquet"
    save_trades(formatted_data, file_path)


async def subscribe_markets(websocket, assets):
    """发送订阅消息到 Polymarket WebSocket"""
    asset_ids = extract_asset_ids(assets)
    await subscribe_asset_ids(websocket, asset_ids)


def extract_asset_ids(assets):
    """从资产数据中提取 asset_id 列表"""
    asset_ids = []
    for coin, interval_data in assets.items():
        for interval, asset_data in interval_data.items():
            if not isinstance(asset_data, dict):
                continue
            # 仅订阅 up 方向的 asset_id
            up_id = asset_data.get("up", "")
            if up_id:
                asset_ids.append(up_id)
    return asset_ids


async def subscribe_asset_ids(websocket, asset_ids):
    """发送 asset_id 列表到订阅接口"""
    if not asset_ids:
        return
    subscribe_msg = {
        "type": "market",
        "assets_ids": asset_ids
    }
    logger.info(f"订阅 {len(asset_ids)} 个市场...")
    await websocket.send(json.dumps(subscribe_msg))


async def send_ping(websocket):
    """每10秒发送一次 PING 保持连接"""
    try:
        while True:
            await asyncio.sleep(10)
            await websocket.send("PING")
    except Exception as e:
        logger.error(f"PING 发送错误: {e}")


async def get_current_assets(target_timestamp=None):
    """获取当前5分钟市场的最新资产信息"""
    coins = ["btc", "eth"]
    assets = {}
    for coin in coins:
        assets[coin] = {}
        coin_data = get_assets(coin, INTERVAL, target_timestamp=target_timestamp)
        if coin_data and coin in coin_data:
            assets[coin][INTERVAL] = coin_data[coin]
        else:
            logger.warning(f"未能获取 {coin}-{INTERVAL} 的市场数据")
    return assets


async def receive_messages(websocket, asset_to_coin, window_open_ts, should_save):
    """接收并处理 WebSocket 消息"""
    while True:
        try:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if message == "PONG":
                continue

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                logger.debug(f"[5m] 忽略非 JSON 消息: {message}")
                continue

            if not isinstance(data, (list, dict)):
                logger.debug(f"[5m] 忽略非市场数据消息类型: {type(data)}")
                continue

            if not should_save():
                continue

            if isinstance(data, list):
                for item in data:
                    if item.get("event_type") == "book":
                        save_book_data([item], asset_to_coin, window_open_ts=window_open_ts)
                    elif item.get("event_type") == "last_trade_price":
                        save_trade_data([item], asset_to_coin, window_open_ts=window_open_ts)
            else:
                if data.get("event_type") == "book":
                    save_book_data([data], asset_to_coin, window_open_ts=window_open_ts)
                elif data.get("event_type") == "last_trade_price":
                    save_trade_data([data], asset_to_coin, window_open_ts=window_open_ts)

        except websockets.ConnectionClosed:
            logger.warning("[5m] WebSocket 连接已关闭")
            break
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
            continue


async def close_ws(websocket, tasks):
    """关闭 websocket 并回收任务"""
    if websocket is None:
        return
    try:
        await websocket.close()
    except Exception:
        pass
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def start_ws(assets, window_open_ts, should_save):
    """建立 WS 连接并启动收包/心跳任务"""
    asset_to_coin = create_asset_mapping(assets)
    websocket = await websockets.connect(WS_URL)
    await subscribe_markets(websocket, assets)
    recv_task = asyncio.create_task(
        receive_messages(websocket, asset_to_coin, window_open_ts, should_save))
    ping_task = asyncio.create_task(send_ping(websocket))
    return websocket, asset_to_coin, [recv_task, ping_task]


async def run_poly_ws_5m(max_retries=999999):
    """运行 5 分钟市场的 WebSocket 连接"""
    retry_count = 0
    # 启动后首个 5m 窗口数据不保存，等到下一个完整窗口开始再保存
    startup_ts = int(time.time())
    current_window_start = (startup_ts // INTERVAL_SECONDS) * INTERVAL_SECONDS
    save_start_timestamp = current_window_start + INTERVAL_SECONDS
    saving_enabled = False
    logger.info(f"[5m] 启动保护：{save_start_timestamp} 之前仅接收不落盘")

    while retry_count < max_retries:
        try:
            logger.info(f"连接到 Polymarket WebSocket: {WS_URL}")

            # 首次连接当前窗口
            assets = await get_current_assets(target_timestamp=current_window_start)
            logger.info("[5m] 连接当前窗口 WS")
            current_ws, current_asset_to_coin, current_tasks = await start_ws(
                assets, current_window_start, lambda: saving_enabled)

            next_ws = None
            next_asset_to_coin = None
            next_tasks = []
            next_window_start = current_window_start + INTERVAL_SECONDS
            next_switch_timestamp = next_window_start
            logger.info(f"[5m] 下一次市场切换时间: {next_switch_timestamp} (剩余 {next_switch_timestamp - int(time.time())} 秒)")

            # 主循环负责按窗口切换，保持单个活动 websocket（不提前预连）
            while True:
                now_timestamp = int(time.time())

                if (not saving_enabled) and now_timestamp >= save_start_timestamp:
                    saving_enabled = True
                    logger.info("[5m] 已进入完整窗口，开始保存 5m 数据")

                # 切换窗口：关闭旧 WS，提升新 WS（如果没有 next_ws 则在切换时建立）
                if now_timestamp >= next_switch_timestamp:
                    if next_ws is None:
                        next_assets = await get_current_assets(target_timestamp=next_window_start)
                        logger.info(f"[5m] 切换时连接下一窗口 WS: {next_window_start}")
                        next_ws, next_asset_to_coin, next_tasks = await start_ws(
                            next_assets, next_window_start, lambda: saving_enabled)

                    await close_ws(current_ws, current_tasks)
                    current_ws = next_ws
                    current_tasks = next_tasks

                    next_ws = None
                    next_tasks = []

                    current_window_start = next_window_start
                    next_window_start = current_window_start + INTERVAL_SECONDS
                    next_switch_timestamp = next_window_start
                    retry_count = 0
                    logger.info(f"[5m] 已进入新一期窗口，下一次切换时间: {next_switch_timestamp}")

                if current_ws is not None:
                    closed = getattr(current_ws, "closed", None)
                    close_code = getattr(current_ws, "close_code", None)
                    retry_count = 0
                    if closed is True or close_code is not None:
                        raise RuntimeError("[5m] 当前 WS 已断开")

                await asyncio.sleep(0.5)


        except Exception as e:
            logger.error(f"连接错误: {e}")
            retry_count += 1
            wait_time = min(5 * retry_count, 60)
            logger.info(f"等待 {wait_time} 秒后重连...")
            await asyncio.sleep(wait_time)
    logger.error("达到最大重试次数，停止尝试连接 Polymarket WebSocket")

if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("启动 Polymarket 5分钟市场数据收集")
        logger.info("=" * 80)
        asyncio.run(run_poly_ws_5m())
    except KeyboardInterrupt:
        logger.info("程序已停止")
