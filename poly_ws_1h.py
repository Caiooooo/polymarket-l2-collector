#!/usr/bin/env python3
"""
Polymarket WebSocket 1小时市场数据获取脚本
"""
import asyncio
import json
import websockets
import time
from binance_price import current_prices
from file_cache import save_trades, save_book
from asset_utils import get_assets
from logger_config import setup_logger

logger = setup_logger('poly_1h')

# Polymarket WebSocket 端点
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
INTERVAL = "1h"
INTERVAL_SECONDS = 60 * 60


def create_asset_mapping(assets):
    """创建 asset_id 到币种的映射"""
    asset_to_coin = {}
    for coin, interval_data in assets.items():
        for interval, asset_data in interval_data.items():
            if isinstance(asset_data, dict):
                asset_to_coin[asset_data.get(
                    "up", "")] = coin.upper()+"_up_"+interval
                asset_to_coin[asset_data.get(
                    "down", "")] = coin.upper()+"_down_"+interval
    return asset_to_coin


def get_asset_price(coin):
    """从 current_prices 获取对应币种的价格"""
    symbol = f"{coin}USDT"
    if symbol in current_prices:
        return current_prices[symbol].get("mid", 0.0)
    return 0.0


def format_orderbook_data(poly_data, asset_to_coin):
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
            "asset_price": asset_price
        }
        formatted_data.append(formatted_item)
    return formatted_data


def format_trade_data(poly_data, asset_to_coin):
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
            "asset_price": asset_price
        }
        formatted_data.append(formatted_item)
    return formatted_data


def save_book_data(data, asset_to_coin):
    """保存格式化的订单簿数据"""
    if not data:
        return
    formatted_data = format_orderbook_data(data, asset_to_coin)
    if not formatted_data:
        return
    asset_id = data[0].get("asset_id", "")
    coin = asset_to_coin.get(asset_id, "")
    if not coin or "_" not in coin:
        return
    now_timestamp = int(time.time())
    now_opening_market = (now_timestamp // INTERVAL_SECONDS) * INTERVAL_SECONDS
    up_or_down = coin.split("_")[1]
    coin_name = coin.split("_")[0]
    file_path = f"data/{INTERVAL}/{coin_name.lower()}/orderbooks/{now_opening_market}{up_or_down}.json"
    save_book(formatted_data, file_path)


def save_trade_data(data, asset_to_coin):
    """保存格式化的交易数据"""
    if not data:
        return
    formatted_data = format_trade_data(data, asset_to_coin)
    if not formatted_data:
        return
    asset_id = data[0].get("asset_id", "")
    coin = asset_to_coin.get(asset_id, "")
    if not coin or "_" not in coin:
        return
    now_timestamp = int(time.time())
    now_opening_market = (now_timestamp // INTERVAL_SECONDS) * INTERVAL_SECONDS
    up_or_down = coin.split("_")[1]
    coin_name = coin.split("_")[0]
    file_path = f"data/{INTERVAL}/{coin_name.lower()}/trades/{now_opening_market}{up_or_down}.json"
    save_trades(formatted_data, file_path)


async def subscribe_markets(websocket, assets):
    """发送订阅消息到 Polymarket WebSocket"""
    asset_ids = []
    for coin, interval_data in assets.items():
        for interval, asset_data in interval_data.items():
            if isinstance(asset_data, dict):
                up_id = asset_data.get("up", "")
                down_id = asset_data.get("down", "")
                if up_id:
                    asset_ids.append(up_id)
                if down_id:
                    asset_ids.append(down_id)
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


async def get_current_assets():
    """获取当前1小时市场的最新资产信息"""
    coins = ["btc", "eth", "sol", "xrp"]
    assets = {}
    for coin in coins:
        assets[coin] = {}
        coin_data = get_assets(coin, INTERVAL)
        if coin_data and coin in coin_data:
            assets[coin][INTERVAL] = coin_data[coin]
        else:
            logger.warning(f"未能获取 {coin}-{INTERVAL} 的市场数据")
    return assets


async def receive_messages(websocket, asset_to_coin, next_switch_timestamp):
    """接收并处理 WebSocket 消息"""
    while True:
        try:
            now_timestamp = int(time.time())
            # 检查是否需要切换到新市场 - 直接返回触发重连
            if now_timestamp >= next_switch_timestamp:
                print(f"\n[1h] ⏰ 到达市场切换时间，断开连接准备重新连接...")
                return  # 直接返回，外层会重新连接

            message = await websocket.recv()

            # 处理 PONG 响应
            if message == "PONG":
                continue

            data = json.loads(message)
            # 检查数据是否是列表（多个市场数据）
            if isinstance(data, list):
                for item in data:
                    if item["event_type"] == "book":
                        save_book_data([item], asset_to_coin)
                    elif item["event_type"] == "last_trade_price":
                        save_trade_data([item], asset_to_coin)
            else:
                if data["event_type"] == "book":
                    save_book_data([data], asset_to_coin)
                elif data["event_type"] == "last_trade_price":
                    save_trade_data([data], asset_to_coin)

        except websockets.ConnectionClosed:
            print("[1h] WebSocket 连接已关闭")
            break
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析错误: {e}")
            continue
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
            continue


async def run_poly_ws_1h(max_retries=999999):
    """运行 1 小时市场的 WebSocket 连接"""
    retry_count = 0
    while retry_count < max_retries:
        try:
            print(f"\n[1h] 连接到 Polymarket WebSocket: {WS_URL}")

            # 每次连接时获取最新的资产信息
            assets = await get_current_assets()
            asset_to_coin = create_asset_mapping(assets)
            logger.info(f"Asset 映射: {len(asset_to_coin)} 个资产")

            # 计算下一个市场切换时间
            now_timestamp = int(time.time())
            next_switch_timestamp = (
                now_timestamp // INTERVAL_SECONDS) * INTERVAL_SECONDS + INTERVAL_SECONDS
            print(
                f"[1h] 下一次市场切换时间: {next_switch_timestamp} (剩余 {next_switch_timestamp - now_timestamp} 秒)")

            # 正确使用 async with 保持连接打开
            async with websockets.connect(WS_URL) as websocket:
                # 发送订阅消息
                await subscribe_markets(websocket, assets)

                # 并行运行 PING 和消息接收
                await asyncio.gather(
                    send_ping(websocket),
                    receive_messages(websocket, asset_to_coin,
                                     next_switch_timestamp),
                    return_exceptions=True
                )

            # 正常退出（时间到达），重置重试计数
            retry_count = 0
            print("[1h] 连接正常关闭，准备重连获取新市场...")
            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"连接错误: {e}")
            retry_count += 1
            wait_time = min(5 * retry_count, 60)
            logger.info(f"等待 {wait_time} 秒后重连...")
            await asyncio.sleep(wait_time)


if __name__ == "__main__":
    try:
        print("=" * 80)
        print("启动 Polymarket 1小时市场数据收集")
        print("=" * 80)
        asyncio.run(run_poly_ws_1h())
    except KeyboardInterrupt:
        print("\n\n[1h] 程序已停止")
