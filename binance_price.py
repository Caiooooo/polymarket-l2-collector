#!/usr/bin/env python3
"""
获取币安 BTC/ETH/SOL/XRP 的 midprice (中间价)
Midprice = (最佳买价 + 最佳卖价) / 2
"""
import asyncio
import json
import random
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

    while True:
        try:
            # ping_interval=20 让库自动发送 WebSocket ping 帧保持连接
            # ping_timeout=10 等待 pong 回复的超时
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_size=2**20,
            ) as websocket:
                retry_count = 0  # 连接成功，重置重试计数
                logger.info("✅ 币安 WebSocket 已连接")

                try:
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
                                    'time': datetime.now().strftime('%H:%M:%S.%f')[:-3]
                                }

                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("币安 WebSocket 连接已关闭，准备重连...")
                            break
                        except Exception as e:
                            logger.error(f"接收数据错误: {e}")
                            break
                except Exception:
                    pass  # 内层异常由外层 while True 处理重连

        except asyncio.CancelledError:
            logger.info("币安订阅任务被取消")
            raise
        except Exception as e:
            retry_count += 1
            # 指数退避 + 随机抖动，最大 120s
            wait_time = min(5 * (2 ** min(retry_count - 1, 4)), 120)
            wait_time += random.uniform(0, wait_time * 0.3)
            logger.warning(f"币安连接失败，{wait_time:.1f}s 后重试 (第 {retry_count} 次): {e}")
            await asyncio.sleep(wait_time)


if __name__ == "__main__":
    try:
        logger.info("启动币安价格订阅（独立模式）")
        asyncio.run(subscribe_book_ticker())
    except KeyboardInterrupt:
        logger.info("程序已停止")
