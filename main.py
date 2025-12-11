import time
import asyncio
import binance_price
import poly_ws_15min
import poly_ws_1h


async def start_new_gathering():
    """同时运行币安价格订阅和两个 Polymarket 市场订阅"""
    await asyncio.gather(
        binance_price.subscribe_book_ticker(),
        poly_ws_15min.run_poly_ws_15min(),
        poly_ws_1h.run_poly_ws_1h(),
        return_exceptions=True
    )


async def main():
    # every 15 minutes/1h restart the websocket
    now_timestamp = int(time.time())
    now_opening_market = (now_timestamp // (15 * 60)) * 15 * 60
    m_close_time = now_opening_market + 15 * 60
    # if now_timestamp >= m_close_time:
    await start_new_gathering()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n程序已停止")
