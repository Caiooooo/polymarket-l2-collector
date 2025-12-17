#!/usr/bin/env python3
"""
47/53 Taker策略 - 在极端价格时主动吃单

策略思路：
当市场开盘时，如果某一方的价格达到极端值（<= 0.47 或 >= 0.53），
说明市场可能过度定价，此时进行反向交易或顺势交易。
"""
import sys
from pathlib import Path
from datetime import datetime, timezone

# 尝试导入，如果失败则添加路径后重试
try:
    from backtest.dataloader import DataLoader
    from backtest.engine import BacktestEngine, Side, OrderType
except ModuleNotFoundError:
    # 添加项目根目录到路径
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from backtest.dataloader import DataLoader
    from backtest.engine import BacktestEngine, Side, OrderType


def take4753_strategy(engine: BacktestEngine, tick: dict):
    """
    47/53 Taker策略

    策略逻辑：
    在开盘前10个tick内，如果价格触及极端值则立即买入
    - ask价格 <= 0.47：买入该方向（认为被低估）
    - ask价格 >= 0.53：买入对手方向（认为对方被高估）

    持有到市场结束，由引擎根据BTC实际涨跌自动结算
    """
    tick_idx = tick['tick_index']

    # 只在开盘前10个tick内观察
    if tick_idx >= 1:
        return
    # print(f"[Strategy] Tick#{tick_idx}")

    # 获取当前持仓（确保每个市场只交易一次）
    if engine.get_position(Side.UP) or engine.get_position(Side.DOWN):
        return

    up_orderbook = tick['up_orderbook']
    down_orderbook = tick['down_orderbook']
    up_ts = up_orderbook['timestamp']
    down_ts = down_orderbook['timestamp']

    # 转换时间戳为 UTC 时间（处理毫秒级时间戳）
    def ts_to_utc(ts):
        if ts > 1e12:  # 毫秒级时间戳
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, timezone.utc)

    up_utc = ts_to_utc(up_ts)
    down_utc = ts_to_utc(down_ts)
    # print(f"[Strategy] up_orderbook: {up_orderbook}")
    # print(f"[Strategy] down_orderbook: {down_orderbook}")

    # 检查是否有有效的订单簿（避免对数组做布尔判断）
    up_asks = up_orderbook.get('asks')
    down_asks = down_orderbook.get('asks')
    if up_asks is None or down_asks is None:
        return
    if len(up_asks) == 0 or len(down_asks) == 0:
        return

    # 获取最优 ask 价格（兼容新旧格式：优先使用 p，退回 price）
    def get_best_ask(order_list):
        best = order_list[-1]
        if 'p' in best:
            return float(best['p']) / 100.0
        return float(best['price'])

    def get_best_ask_sz(order_list):
        best = order_list[-1]
        if 'p' in best:
            return float(best['s']) / 100.0
        return float(best['size'])

    up_ask = get_best_ask(up_asks)
    up_sz = get_best_ask_sz(up_asks)
    down_ask = get_best_ask(down_asks)
    down_sz = get_best_ask_sz(down_asks)

    # 阈值
    LOW_THRESHOLD = 0.47   # 低于此价格认为被低估，买入
    GROUND_THRESHOLD = 0.45  # 低于此价格认为价格有异常，观望

    size = 10.0  # 每次交易数量

    # 检查UP的价格
    if up_ask <= LOW_THRESHOLD and up_ask >= GROUND_THRESHOLD and up_sz > size:
        # UP价格很低，买入UP
        order = engine.place_order(
            Side.UP, size=size, order_type=OrderType.MARKET)
        if order and order.is_filled():
            print(
                f"[Strategy] Tick#{tick_idx} | UTC: {up_utc.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | UP价格 {up_ask:.4f} <= {LOW_THRESHOLD}, 买入UP {size:.0f}")
        return

    # 检查DOWN的价格
    if down_ask <= LOW_THRESHOLD and down_ask >= GROUND_THRESHOLD and down_sz > size:
        # DOWN价格很低，买入DOWN
        order = engine.place_order(
            Side.DOWN, size=size, order_type=OrderType.MARKET)
        if order and order.is_filled():
            print(
                f"[Strategy] Tick#{tick_idx} | UTC: {down_utc.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} | DOWN价格 {down_ask:.4f} <= {LOW_THRESHOLD}, 买入DOWN {size:.0f}")
        return


def main():
    """主函数"""
    # 设置回测时间范围
    start_time = datetime(2025, 12, 13, 0, 0)
    end_time = datetime(2027, 12, 15, 23, 59)

    # 增加初始资金以支持更多交易
    initial_balance = 1000.0

    print("=" * 60)
    print("回测配置:")
    print(f"  开始时间: {start_time}")
    print(f"  结束时间: {end_time}")
    print(f"  初始资金: ${initial_balance:.2f}")
    print("=" * 60)

    # 初始化数据加载器
    loader = DataLoader(start_time, end_time)

    # 初始化回测引擎
    engine = BacktestEngine(
        dataloader=loader,
        initial_balance=initial_balance,
        fee_rate=0.0,
        verbose=False  # 关闭详细输出，提高速度
    )

    # 设置策略
    engine.set_strategy(take4753_strategy)  # 使用47/53阈值策略

    # 运行回测
    engine.run()


if __name__ == "__main__":
    main()
