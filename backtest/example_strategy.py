#!/usr/bin/env python3
"""
示例策略 - 展示如何使用回测引擎
"""
from datetime import datetime
from dataloader import DataLoader
from engine import BacktestEngine, Side, OrderType


def momentum_strategy(engine: BacktestEngine, tick: dict):
    """
    简单价值策略

    策略逻辑：
    在每个市场开始时，买入价格较低的一方
    持有到市场结束，由引擎根据BTC实际涨跌自动结算
    """
    tick_idx = tick['tick_index']

    # 只在市场开始时（第20个tick）买入一次
    if tick_idx != 20:
        return

    up_orderbook = tick['up_orderbook']
    down_orderbook = tick['down_orderbook']

    # 检查是否有有效的订单簿（避免对数组做布尔判断）
    up_asks = up_orderbook.get('asks')
    down_asks = down_orderbook.get('asks')
    if up_asks is None or down_asks is None:
        return
    if len(up_asks) == 0 or len(down_asks) == 0:
        return

    # 兼容新旧格式：优先使用 p（整数*100），否则回退到 price（浮点）
    def get_best_ask(order_list):
        best = order_list[-1]
        if 'p' in best:
            return float(best['p']) / 100.0
        return float(best['price'])

    up_ask = get_best_ask(up_asks)
    down_ask = get_best_ask(down_asks)

    # 获取当前持仓（确保每个市场只交易一次）
    if engine.get_position(Side.UP) or engine.get_position(Side.DOWN):
        return

    # 买入价格相对较低的一方（认为被低估）
    if up_ask < down_ask:
        # UP 价格较低，买入UP
        size = 10.0
        order = engine.place_order(
            Side.UP, size=size, order_type=OrderType.MARKET)
        if order and order.is_filled():
            print(
                f"[Strategy] 买入 UP {size:.0f} @ ${up_ask:.4f} (DOWN: ${down_ask:.4f})")
    else:
        # DOWN 价格较低，买入DOWN
        size = 10.0
        order = engine.place_order(
            Side.DOWN, size=size, order_type=OrderType.MARKET)
        if order and order.is_filled():
            print(
                f"[Strategy] 买入 DOWN {size:.0f} @ ${down_ask:.4f} (UP: ${up_ask:.4f})")


def main():
    """主函数"""
    # 设置回测时间范围
    start_time = datetime(2025, 12, 15, 0, 0)
    end_time = datetime(2025, 12, 16, 23, 59)

    # 增加初始资金以支持更多交易
    initial_balance = 10000.0

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
    engine.set_strategy(momentum_strategy)  # 使用动量策略
    # engine.set_strategy(simple_strategy)  # 或使用简单策略

    # 运行回测
    engine.run()


if __name__ == "__main__":
    main()
