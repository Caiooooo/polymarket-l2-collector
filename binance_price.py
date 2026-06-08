#!/usr/bin/env python3
"""
获取币安 BTC/ETH/SOL/XRP 的 midprice (中间价)
Midprice = (最佳买价 + 最佳卖价) / 2
"""
import asyncio
import json
import random
import time
import websockets
from datetime import datetime
from logger_config import setup_logger

logger = setup_logger('binance')


# 币安 WebSocket 端点
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"

# 要订阅的交易对
SYMBOLS = ["btcusdt", "ethusdt"]

# 存储当前价格
current_prices = {}
# 最后收到消息的时间戳（供 main.py 健康检查用）
last_message_time = 0.0


async def subscribe_book_ticker():
    """订阅币安 bookTicker 流获取最佳买卖价，支持自动重连"""

    # 构建订阅流
    streams = "/".join([f"{symbol}@bookTicker" for symbol in SYMBOLS])
    ws_url = f"{BINANCE_WS_URL}?streams={streams}"

    retry_count = 0

    while True:
        try:
            # ping_interval=20: 客户端每 20s 发送 WebSocket PING 帧（穿透代理/NAT）
            # ping_timeout=10: 等待 PONG 的超时，超时则视为断开
            # 库自带自动回复服务端 PING 的能力，无需手动处理
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                max_size=2**19,         # 512KB，低内存优化
            ) as websocket:
                retry_count = 0
                logger.info("✅ 币安 WebSocket 已连接")

                # 应用层双保险保活：定期发送 unsolicited PONG 帧
                keepalive_task = asyncio.create_task(
                    _keepalive_pong(websocket))

                try:
                    while True:
                        try:
                            message = await websocket.recv()
                            global last_message_time
                            last_message_time = time.time()
                            data = json.loads(message)

                            if 'data' in data:
                                stream_data = data['data']
                                symbol = stream_data['s']
                                best_bid = float(stream_data['b'])
                                best_ask = float(stream_data['a'])
                                midprice = (best_bid + best_ask) / 2

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
                finally:
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except asyncio.CancelledError:
                        pass

        except asyncio.CancelledError:
            logger.info("币安订阅任务被取消")
            raise
        except Exception as e:
            retry_count += 1
            wait_time = min(5 * (2 ** min(retry_count - 1, 4)), 120)
            wait_time += random.uniform(0, wait_time * 0.3)
            logger.warning(f"币安连接失败，{wait_time:.1f}s 后重试 (第 {retry_count} 次): {e}")
            await asyncio.sleep(wait_time)


async def _keepalive_pong(websocket):
    """应用层保活：每 30s 发送 unsolicited PONG 帧，作为 ping_interval 的双保险。

    Binance 服务端每 3 分钟发一次 PING，websockets 库会自动回复 PONG。
    这里的额外 PONG 用于穿透某些 aggressive 的中间代理/NAT 超时。
    """
    try:
        while True:
            await asyncio.sleep(30)
            await websocket.pong()
    except websockets.exceptions.ConnectionClosed:
        pass
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.debug(f"保活 PONG 发送失败: {e}")


if __name__ == "__main__":
    try:
        logger.info("启动币安价格订阅（独立模式）")
        asyncio.run(subscribe_book_ticker())
    except KeyboardInterrupt:
        logger.info("程序已停止")
