#!/usr/bin/env python3
"""
BacktestEngine - 回测引擎
用于回测 Polymarket 15分钟 BTC up/down 市场交易策略
"""
import copy
from datetime import datetime
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum


class Side(Enum):
    """交易方向"""
    UP = "up"      # 买 UP
    DOWN = "down"  # 买 DOWN


class OrderType(Enum):
    """订单类型"""
    MARKET = "market"  # 市价单
    LIMIT = "limit"    # 限价单


@dataclass
class Order:
    """订单"""
    order_id: int
    side: Side              # UP 或 DOWN
    order_type: OrderType   # 市价或限价
    size: float            # 数量
    price: Optional[float] = None  # 限价单价格
    filled_size: float = 0.0       # 已成交数量
    avg_fill_price: float = 0.0    # 平均成交价
    status: str = "pending"        # pending, filled, partial, cancelled
    create_time: datetime = None
    fill_time: datetime = None

    def is_filled(self) -> bool:
        return self.status == "filled"

    def is_partial(self) -> bool:
        return self.status == "partial"


@dataclass
class Position:
    """持仓"""
    side: Side              # UP 或 DOWN
    size: float            # 持仓数量
    entry_price: float     # 入场价格
    current_price: float   # 当前价格
    pnl: float = 0.0       # 盈亏

    def update_pnl(self, current_price: float):
        """更新盈亏"""
        self.current_price = current_price
        # Polymarket: 买入价格低，卖出价格高则盈利
        self.pnl = (current_price - self.entry_price) * self.size


@dataclass
class Trade:
    """成交记录"""
    trade_id: int
    order_id: int
    side: Side
    size: float
    price: float
    timestamp: datetime
    market_time: datetime


class BacktestEngine:
    """
    回测引擎
    """

    def __init__(self,
                 dataloader,
                 initial_balance: float = 10000.0,
                 fee_rate: float = 0.0,  # Polymarket 目前无手续费
                 verbose: bool = True):
        """
        初始化回测引擎

        Args:
            dataloader: DataLoader 实例
            initial_balance: 初始资金
            fee_rate: 手续费率
            verbose: 是否打印详细信息
        """
        self.dataloader = dataloader
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.fee_rate = fee_rate
        self.verbose = verbose

        # 持仓管理
        self.positions: Dict[Side, Position] = {}  # 当前持仓
        self.orders: List[Order] = []               # 所有订单
        self.trades: List[Trade] = []               # 所有成交

        # ID 计数器
        self.next_order_id = 1
        self.next_trade_id = 1

        # 当前市场状态
        self.current_tick = None
        self.current_market = None

        # 策略回调
        self.strategy_callback: Optional[Callable] = None

        # 统计信息
        self.total_pnl = 0.0
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0

        # 市场快照（用于验证失败时回滚）
        self.market_snapshot = None

        print(f"[Engine] 回测引擎初始化完成")
        print(f"[Engine] 初始资金: ${initial_balance:.2f}")

    def set_strategy(self, strategy_func: Callable):
        """
        设置策略回调函数

        Args:
            strategy_func: 策略函数，签名为 strategy_func(engine, tick_data)
        """
        self.strategy_callback = strategy_func

    def place_order(self,
                    side: Side,
                    size: float,
                    order_type: OrderType = OrderType.MARKET,
                    price: Optional[float] = None) -> Optional[Order]:
        """
        下单

        Args:
            side: UP 或 DOWN
            size: 数量
            order_type: 订单类型
            price: 限价单价格

        Returns:
            Order 对象，如果下单失败返回 None
        """
        if size <= 0:
            print(f"[Engine] 下单失败: 数量必须大于0")
            return None

        # 检查资金
        if order_type == OrderType.MARKET:
            # 市价单：使用最佳卖价估算成本
            estimated_cost = self._estimate_order_cost(
                side, size, order_type, price)
            if estimated_cost > self.balance:
                print(
                    f"[Engine] 下单失败: 资金不足 (需要 ${estimated_cost:.2f}, 可用 ${self.balance:.2f})")
                return None

        order = Order(
            order_id=self.next_order_id,
            side=side,
            order_type=order_type,
            size=size,
            price=price,
            create_time=datetime.now()
        )
        self.next_order_id += 1
        self.orders.append(order)

        # 尝试立即成交
        self._try_fill_order(order)

        return order

    def _estimate_order_cost(self, side: Side, size: float, order_type: OrderType, price: Optional[float]) -> float:
        """估算订单成本"""
        if order_type == OrderType.LIMIT and price:
            return size * price

        # 市价单：使用当前最佳卖价（兼容 p/price）
        if self.current_tick:
            orderbook = self.current_tick[f'{side.value}_orderbook']
            if 'asks' in orderbook and len(orderbook['asks']) > 0:
                best_ask = orderbook['asks'][-1]
                if 'p' in best_ask:
                    best_ask_price = float(best_ask['p']) / 100.0
                else:
                    best_ask_price = float(best_ask['price'])
                return size * best_ask_price

        # 保守估计：假设价格为 0.5
        return size * 0.5

    def _try_fill_order(self, order: Order):
        """
        尝试成交订单

        Args:
            order: 订单对象
        """
        if not self.current_tick:
            return

        orderbook = self.current_tick[f'{order.side.value}_orderbook']

        if order.order_type == OrderType.MARKET:
            # 市价单：与 asks 成交（我们是买方）
            if 'asks' not in orderbook or len(orderbook['asks']) == 0:
                order.status = "cancelled"
                if self.verbose:
                    print(f"[Engine] 订单 {order.order_id} 取消: 无卖单")
                return

            remaining_size = order.size
            total_cost = 0.0

            # 倒序遍历asks，从最优价格（末尾）开始撮合
            for ask in reversed(orderbook['asks']):
                # 兼容 p/s 与 price/size
                if 'p' in ask:
                    ask_price = float(ask['p']) / 100.0
                else:
                    ask_price = float(ask['price'])

                if 's' in ask:
                    ask_size = float(ask['s']) / 100.0
                else:
                    ask_size = float(ask['size'])

                fill_size = min(remaining_size, ask_size)
                cost = fill_size * ask_price

                if cost > self.balance:
                    # 资金不足，部分成交
                    fill_size = self.balance / ask_price
                    cost = self.balance

                if fill_size > 0:
                    self._execute_fill(order, fill_size, ask_price, cost)
                    remaining_size -= fill_size

                if remaining_size <= 0.001 or self.balance < 0.01:  # 完全成交或资金用尽
                    break

            # 更新订单状态
            if order.filled_size >= order.size * 0.999:  # 允许微小误差
                order.status = "filled"
                order.fill_time = datetime.now()
            elif order.filled_size > 0:
                order.status = "partial"
            else:
                order.status = "cancelled"

        elif order.order_type == OrderType.LIMIT:
            # 限价单：检查是否有合适的对手盘
            if 'asks' not in orderbook:
                return

            # 倒序遍历asks，从最优价格（末尾）开始撮合
            for ask in reversed(orderbook['asks']):
                # 兼容 p/s 与 price/size
                if 'p' in ask:
                    ask_price = float(ask['p']) / 100.0
                else:
                    ask_price = float(ask['price'])

                if 's' in ask:
                    ask_size = float(ask['s']) / 100.0
                else:
                    ask_size = float(ask['size'])

                if ask_price <= order.price:  # 价格合适
                    fill_size = min(order.size - order.filled_size, ask_size)
                    cost = fill_size * ask_price

                    if cost <= self.balance:
                        self._execute_fill(order, fill_size, ask_price, cost)

                    if order.filled_size >= order.size * 0.999:
                        order.status = "filled"
                        order.fill_time = datetime.now()
                        break

    def _execute_fill(self, order: Order, fill_size: float, fill_price: float, cost: float):
        """
        执行订单成交

        Args:
            order: 订单
            fill_size: 成交数量
            fill_price: 成交价格
            cost: 成交金额
        """
        # 扣除资金
        self.balance -= cost

        # 更新订单
        order.filled_size += fill_size
        order.avg_fill_price = ((order.avg_fill_price * (order.filled_size - fill_size)) +
                                (fill_price * fill_size)) / order.filled_size

        # 记录成交
        trade = Trade(
            trade_id=self.next_trade_id,
            order_id=order.order_id,
            side=order.side,
            size=fill_size,
            price=fill_price,
            timestamp=datetime.now(),
            market_time=self.current_tick['market_time']
        )
        self.next_trade_id += 1
        self.trades.append(trade)
        self.total_trades += 1

        # 更新持仓
        self._update_position(order.side, fill_size, fill_price)

        if self.verbose:
            print(f"[Engine] 成交: {order.side.value.upper()} {fill_size:.2f} @ ${fill_price:.4f}, "
                  f"订单ID: {order.order_id}, 余额: ${self.balance:.2f}")

    def _update_position(self, side: Side, size: float, price: float):
        """
        更新持仓

        Args:
            side: UP 或 DOWN
            size: 增加的数量
            price: 成交价格
        """
        if side in self.positions:
            # 更新现有持仓
            pos = self.positions[side]
            total_size = pos.size + size
            pos.entry_price = ((pos.entry_price * pos.size) +
                               (price * size)) / total_size
            pos.size = total_size
        else:
            # 创建新持仓
            self.positions[side] = Position(
                side=side,
                size=size,
                entry_price=price,
                current_price=price
            )

    def close_position(self, side: Side, size: Optional[float] = None) -> bool:
        """
        平仓

        Args:
            side: UP 或 DOWN
            size: 平仓数量，None 表示全部平仓

        Returns:
            是否成功平仓
        """
        if side not in self.positions:
            print(f"[Engine] 平仓失败: 无 {side.value.upper()} 持仓")
            return False

        position = self.positions[side]
        close_size = size if size else position.size

        if close_size > position.size:
            print(
                f"[Engine] 平仓失败: 数量超过持仓 (持仓 {position.size}, 请求 {close_size})")
            return False

        # 获取当前卖价（我们卖出，使用 bids）
        if not self.current_tick:
            return False

        orderbook = self.current_tick[f'{side.value}_orderbook']
        if 'bids' not in orderbook or len(orderbook['bids']) == 0:
            print(f"[Engine] 平仓失败: 无买单")
            return False

        # 按最佳买价卖出
        best_bid_price = float(orderbook['bids'][-1]['price'])
        proceeds = close_size * best_bid_price

        # 更新资金
        self.balance += proceeds

        # 计算盈亏
        # print(f"best_bid_price: {best_bid_price}, position.entry_price: {position.entry_price}, close_size: {close_size}")
        pnl = (best_bid_price - position.entry_price) * close_size
        self.total_pnl += pnl

        if pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1

        # 更新持仓
        position.size -= close_size
        if position.size < 0.001:  # 完全平仓
            del self.positions[side]

        if self.verbose:
            print(f"[Engine] 平仓: {side.value.upper()} {close_size:.2f} @ ${best_bid_price:.4f}, "
                  f"盈亏: ${pnl:.2f}, 余额: ${self.balance:.2f}")

        return True

    def close_all_positions(self):
        """平掉所有持仓"""
        for side in list(self.positions.keys()):
            self.close_position(side)

    def _settle_market(self, market_data: Dict):
        """
        市场结算（15分钟到期）
        根据BTC实际价格涨跌进行结算：
        - 价格上涨: UP结算为$1.0，DOWN结算为$0
        - 价格下跌: DOWN结算为$1.0，UP结算为$0

        Args:
            market_data: 市场数据
        """
        if len(self.positions) == 0:
            return

        if self.verbose:
            print(f"\n[Engine] ========== 市场结算 ==========")
            dt = market_data['datetime']
            dt_trimmed = dt.replace(second=0, microsecond=0)
            unix_ts = int(dt_trimmed.timestamp())
            print(
                f"[Engine] 市场时间: {dt_trimmed.strftime('%Y-%m-%d %H:%M')} (Unix: {unix_ts})")

        # 获取市场开始和结束时的BTC价格
        up_data = market_data['up']
        down_data = market_data['down']

        if len(up_data) == 0 or len(down_data) == 0:
            if self.verbose:
                print(f"[Engine] 警告: 无法结算，数据为空")
            return

        # 从第一个和最后一个tick获取BTC价格
        start_btc_price = None
        end_btc_price = None

        # 尝试从up数据获取
        if 'asset_price' in up_data[0]:
            start_btc_price = float(up_data[0]['asset_price'])
        if 'asset_price' in up_data[-1]:
            end_btc_price = float(up_data[-1]['asset_price'])

        # 如果up数据没有，尝试从down数据获取
        if start_btc_price is None and 'asset_price' in down_data[0]:
            start_btc_price = float(down_data[0]['asset_price'])
        if end_btc_price is None and 'asset_price' in down_data[-1]:
            end_btc_price = float(down_data[-1]['asset_price'])

        if start_btc_price is None or end_btc_price is None:
            if self.verbose:
                print(f"[Engine] 警告: 无法获取BTC价格，使用市场价格结算")
            # 降级为使用市场价格结算
            for side, position in list(self.positions.items()):
                self.close_position(side)
            if self.verbose:
                print(f"[Engine] 结算完成, 当前余额: ${self.balance:.2f}")
                print(f"[Engine] ==================================\n")
            return

        # 计算BTC价格变化
        btc_change = end_btc_price - start_btc_price
        btc_change_pct = (btc_change / start_btc_price) * 100
        if self.verbose:
            print(
                f"[Engine] BTC变化: {btc_change:+.2f} ({btc_change_pct:+.2f}%)")

        # 判断涨跌并结算
        if btc_change > 0:
            # BTC上涨 - UP赢，DOWN输
            if self.verbose:
                print(f"[Engine] 结算结果: UP = $1.00, DOWN = $0.00")
            settlement_prices = {Side.UP: 1.0, Side.DOWN: 0.0}
            expected_winner = Side.UP
        else:
            # BTC下跌或持平 - DOWN赢，UP输
            if self.verbose:
                print(f"[Engine] 结算结果: DOWN = $1.00, UP = $0.00")
            settlement_prices = {Side.UP: 0.0, Side.DOWN: 1.0}
            expected_winner = Side.DOWN

        # 交叉验证：通过orderbook的bids长度验证结果
        # up_data 和 down_data 数组中每个元素直接就是 orderbook 对象
        up_orderbook = up_data[-1] if len(up_data) > 0 else {}
        down_orderbook = down_data[-1] if len(down_data) > 0 else {}
        up_bids_len = len(up_orderbook.get('bids', []))
        down_bids_len = len(down_orderbook.get('bids', []))
        up_asks_len = len(up_orderbook.get('asks', []))
        down_asks_len = len(down_orderbook.get('asks', []))

        # 根据bids长度判断实际获胜方
        actual_winner = Side.UP if up_bids_len > down_bids_len else Side.DOWN

        # 检查是否有稀疏数据（某方已经接近输光）
        up_sparse = up_bids_len <= 1 or up_asks_len <= 1
        down_sparse = down_bids_len <= 1 or down_asks_len <= 1
        is_sparse = up_sparse or down_sparse

        # 如果数据稀疏，相信orderbook的结果
        if is_sparse:
            if actual_winner != expected_winner:
                # 使用actual_winner重新设置settlement_prices
                if actual_winner == Side.UP:
                    settlement_prices = {Side.UP: 1.0, Side.DOWN: 0.0}
                    # print(f"[Engine] 修正结算结果: UP = $1.00, DOWN = $0.00")
                else:
                    settlement_prices = {Side.UP: 0.0, Side.DOWN: 1.0}
                    # print(f"[Engine] 修正结算结果: DOWN = $1.00, UP = $0.00")
        else:
            # 数据不稀疏，如果验证结果不一致，作废本次市场的交易
            if actual_winner != expected_winner:
                print(f"[Engine] 交叉验证: UP (bids={up_bids_len}, asks={up_asks_len}), "
                      f"DOWN (bids={down_bids_len}, asks={down_asks_len}), "
                      f"实际获胜={actual_winner.value.upper()}")
                print(f"[Engine] ⚠️  验证失败！预期获胜={expected_winner.value.upper()}, "
                      f"实际获胜={actual_winner.value.upper()}")
                print(f"[Engine] 作废本次市场的所有交易，恢复市场开始前的状态")

                # 恢复快照
                if self.market_snapshot:
                    self.balance = self.market_snapshot['balance']
                    self.positions = self.market_snapshot['positions'].copy()
                    self.total_trades = self.market_snapshot['total_trades']
                    self.winning_trades = self.market_snapshot['winning_trades']
                    self.losing_trades = self.market_snapshot['losing_trades']
                    self.total_pnl = self.market_snapshot['total_pnl']
                    print(f"[Engine] 已恢复余额: ${self.balance:.2f}")

                print(f"[Engine] ==================================\n")
                return

        # 结算所有持仓
        for side, position in list(self.positions.items()):
            settlement_price = settlement_prices[side]
            proceeds = position.size * settlement_price

            # 计算盈亏
            cost = position.size * position.entry_price
            pnl = proceeds - cost
            self.total_pnl += pnl

            if pnl > 0:
                self.winning_trades += 1
            else:
                self.losing_trades += 1

            # 更新资金
            self.balance += proceeds

            if self.verbose:
                print(f"[Engine] 结算 {side.value.upper()}: "
                      f"持仓 {position.size:.2f} @ ${position.entry_price:.4f}, "
                      f"结算价 ${settlement_price:.2f}, "
                      f"盈亏 ${pnl:+.2f}")

            # 删除持仓
            del self.positions[side]

        if self.verbose:
            print(f"[Engine] 结算完成, 当前余额: ${self.balance:.2f}")
            print(f"[Engine] ==================================\n")

    def run(self):
        """
        运行回测
        """
        print(f"\n[Engine] ========== 开始回测 ==========\n")

        # 遍历所有市场
        while self.dataloader.has_next_market():
            market = self.dataloader.next_market()
            if not market:
                break

            self.current_market = market
            # print(f"[Engine] 进入市场: {market['datetime']}")

            # 保存市场开始前的状态快照（用于验证失败时回滚）
            self.market_snapshot = {
                'balance': self.balance,
                'positions': copy.deepcopy(self.positions),
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'losing_trades': self.losing_trades,
                'total_pnl': self.total_pnl
            }

            # 遍历所有 ticks
            while self.dataloader.has_next_tick():
                tick = self.dataloader.next_tick()
                if not tick:
                    break

                self.current_tick = tick

                # 更新持仓价格和盈亏
                for side, position in self.positions.items():
                    orderbook = tick[f'{side.value}_orderbook']
                    if 'bids' in orderbook and len(orderbook['bids']) > 0:
                        best_bid = orderbook['bids'][-1]
                        # 兼容新旧格式：优先使用 p（整数*100），否则回退到 price（浮点）
                        if 'p' in best_bid:
                            current_price = float(best_bid['p']) / 100.0
                        else:
                            current_price = float(best_bid['price'])
                        position.update_pnl(current_price)

                # 执行策略
                if self.strategy_callback:
                    self.strategy_callback(self, tick)

            # 市场结束，自动结算
            self._settle_market(market)

        # 回测结束
        self._print_summary()

    def _print_summary(self):
        """打印回测总结"""
        print(f"\n[Engine] ========== 回测总结 ==========")
        print(f"[Engine] 初始资金: ${self.initial_balance:.2f}")
        print(f"[Engine] 最终资金: ${self.balance:.2f}")
        print(f"[Engine] 总盈亏: ${self.balance - self.initial_balance:.2f}")
        print(
            f"[Engine] 收益率: {(self.balance / self.initial_balance - 1) * 100:.2f}%")
        print(f"[Engine] 总交易次数: {self.total_trades}")
        print(f"[Engine] 盈利交易: {self.winning_trades}")
        print(f"[Engine] 亏损交易: {self.losing_trades}")
        if self.total_trades > 0:
            win_rate = self.winning_trades / \
                (self.winning_trades + self.losing_trades) * 100
            print(f"[Engine] 胜率: {win_rate:.2f}%")
        print(f"[Engine] =====================================\n")

    def get_portfolio_value(self) -> float:
        """
        获取当前组合价值（资金 + 持仓市值）

        Returns:
            组合总价值
        """
        portfolio_value = self.balance

        for side, position in self.positions.items():
            # 使用当前买价估值
            if self.current_tick:
                orderbook = self.current_tick[f'{side.value}_orderbook']
                if 'bids' in orderbook and len(orderbook['bids']) > 0:
                    current_price = float(orderbook['bids'][-1]['price'])
                    portfolio_value += position.size * current_price

        return portfolio_value

    def get_position(self, side: Side) -> Optional[Position]:
        """获取指定方向的持仓"""
        return self.positions.get(side)

    def get_total_position_size(self, side: Side) -> float:
        """获取指定方向的总持仓量"""
        if side in self.positions:
            return self.positions[side].size
        return 0.0


if __name__ == "__main__":
    # 测试代码
    from datetime import datetime
    from dataloader import DataLoader

    # 设置测试时间范围
    start = datetime(2025, 12, 15, 0, 0)
    end = datetime(2025, 12, 15, 23, 59)

    # 初始化
    loader = DataLoader(start, end)
    engine = BacktestEngine(loader, initial_balance=1000.0)

    # 简单的测试策略：随机买入
    def simple_strategy(engine, tick):
        # 每50个tick买入一次
        if tick['tick_index'] % 10 == 0:
            # 买入 UP
            engine.place_order(Side.UP, size=1.0, order_type=OrderType.MARKET)

    # 设置策略并运行
    engine.set_strategy(simple_strategy)
    engine.run()
