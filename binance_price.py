#!/usr/bin/env python3
"""
获取币安 BTC/ETH/SOL/XRP 的 midprice (中间价)
Midprice = (最佳买价 + 最佳卖价) / 2
"""
import asyncio
import json
import websockets
from datetime import datetime
from logger_config import setup_logger

logger = setup_logger('binance')


# 币安 WebSocket 端点
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"

# 要订阅的交易对
SYMBOLS = ["btcusdt", "ethusdt", "solusdt", "xrpusdt"]

# 存储当前价格
current_prices = {}


async def subscribe_book_ticker():
    """订阅币安 bookTicker 流获取最佳买卖价，支持自动重连"""

    # 构建订阅流
    streams = "/".join([f"{symbol}@bookTicker" for symbol in SYMBOLS])
    ws_url = f"{BINANCE_WS_URL}?streams={streams}"

    retry_count = 0
    max_retries = 999999  # 无限重试

    while retry_count < max_retries:
        try:

            async with websockets.connect(ws_url) as websocket:
                retry_count = 0  # 连接成功，重置重试计数

                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)

                        # 解析数据
                        if 'data' in data:
                            stream_data = data['data']
                            symbol = stream_data['s']  # 交易对 如 BTCUSDT
                            best_bid = float(stream_data['b'])  # 最佳买价
                            best_ask = float(stream_data['a'])  # 最佳卖价

                            # 计算 midprice
                            midprice = (best_bid + best_ask) / 2

                            # 保存当前价格
                            current_prices[symbol] = {
                                'bid': best_bid,
                                'ask': best_ask,
                                'mid': midprice,
                                'spread': best_ask - best_bid,
                                # 基点
                                'spread_bps': ((best_ask - best_bid) / midprice) * 10000,
                                'time': datetime.now().strftime('%H:%M:%S.%f')[:-3]
                            }

                            # 打印当前所有价格
                            # print(
                            #     f"\r{datetime.now().strftime('%H:%M:%S.%f')[:-3]}", end=" | ")

                            for sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT']:
                                if sym in current_prices:
                                    p = current_prices[sym]
                                    # print(
                                    #     f"{sym[:3]}: ${p['mid']:>10,.2f}", end=" | ")

                            # print("", end="\r", flush=True)

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("WebSocket 连接已关闭，准备重连...")
                        break
                    except Exception as e:
                        logger.error(f"接收数据错误: {e}")
                        break

        except Exception as e:
            retry_count += 1
            wait_time = min(5 * retry_count, 600)  # 最多等待600秒
            await asyncio.sleep(wait_time)


async def print_detailed_prices():
    """定期打印详细价格信息"""
    await asyncio.sleep(2)  # 等待连接建立

    while True:
        await asyncio.sleep(5)  # 每5秒打印一次详细信息

        if current_prices:

            for symbol in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT']:
                if symbol in current_prices:
                    p = current_prices[symbol]


async def main():
    """同时运行价格订阅和详细打印"""
    await asyncio.gather(
        subscribe_book_ticker(),
        print_detailed_prices()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序已停止")
