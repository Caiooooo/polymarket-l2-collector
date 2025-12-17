# Polymarket 回测引擎

用于回测 Polymarket 15分钟 BTC up/down 市场的交易策略。

## 文件结构

```
backtest/
├── dataloader.py          # 数据加载器
├── engine.py              # 回测引擎
├── example_strategy.py    # 示例策略
└── strategy/              # 策略目录
    └── take4753.py
```

## 快速开始

### 1. DataLoader（数据加载器）

`DataLoader` 类负责加载历史 orderbook 数据。

```python
from datetime import datetime
from dataloader import DataLoader

# 创建数据加载器
loader = DataLoader(
    start_time=datetime(2025, 12, 13, 0, 0),
    end_time=datetime(2025, 12, 15, 23, 59),
    data_dir="/root/poly/data/15m/btc/orderbooks"
)

# 加载下一个市场（15分钟）
market = loader.next_market()
print(f"市场时间: {market['datetime']}")
print(f"总 ticks: {market['total_ticks']}")

# 加载下一个 tick
tick = loader.next_tick()
print(f"UP orderbook: {tick['up_orderbook']}")
print(f"DOWN orderbook: {tick['down_orderbook']}")
```

#### DataLoader 主要方法：

- `has_next_market()` - 检查是否还有下一个市场
- `next_market()` - 加载下一个15分钟市场的所有数据
- `has_next_tick()` - 检查当前市场是否还有下一个tick
- `next_tick()` - 加载当前市场的下一个tick orderbook
- `get_progress()` - 获取回测进度

### 2. BacktestEngine（回测引擎）

`BacktestEngine` 类提供完整的回测功能，包括下单、持仓管理、自动结算等。

```python
from datetime import datetime
from dataloader import DataLoader
from engine import BacktestEngine, Side, OrderType

# 创建回测引擎
loader = DataLoader(start_time, end_time)
engine = BacktestEngine(
    dataloader=loader,
    initial_balance=1000.0,  # 初始资金
    fee_rate=0.0,            # 手续费率
    verbose=True             # 打印详细信息
)

# 定义策略函数
def my_strategy(engine, tick):
    # 获取 orderbook 数据
    up_orderbook = tick['up_orderbook']
    down_orderbook = tick['down_orderbook']
    
    # 获取价格
    up_price = float(up_orderbook['asks'][0]['price'])
    down_price = float(down_orderbook['asks'][0]['price'])
    
    # 交易逻辑
    if up_price < 0.40:
        engine.place_order(Side.UP, size=100.0)
    
    # 止盈止损
    position = engine.get_position(Side.UP)
    if position:
        pnl_pct = (up_price - position.entry_price) / position.entry_price
        if pnl_pct > 0.10:  # 止盈10%
            engine.close_position(Side.UP)

# 设置策略并运行
engine.set_strategy(my_strategy)
engine.run()
```

#### BacktestEngine 主要方法：

- `place_order(side, size, order_type, price)` - 下单
  - `side`: `Side.UP` 或 `Side.DOWN`
  - `order_type`: `OrderType.MARKET` 或 `OrderType.LIMIT`
- `close_position(side, size)` - 平仓
- `close_all_positions()` - 平掉所有持仓
- `get_position(side)` - 获取指定方向的持仓
- `get_total_position_size(side)` - 获取持仓量
- `get_portfolio_value()` - 获取组合总价值（资金+持仓）

### 3. 运行示例策略

```bash
cd /root/poly/backtest
python3 example_strategy.py
```

## 策略开发指南

### 策略函数签名

```python
def strategy_function(engine: BacktestEngine, tick: dict):
    """
    策略函数
    
    Args:
        engine: BacktestEngine 实例，用于下单和查询状态
        tick: 当前 tick 数据，包含：
            - market_time: 市场开始时间
            - tick_index: 当前tick索引
            - up_orderbook: UP的orderbook
            - down_orderbook: DOWN的orderbook
    """
    pass
```

### Orderbook 数据结构

```python
{
    'bids': [
        {'price': '0.55', 'size': '1000'},
        {'price': '0.54', 'size': '2000'},
        ...
    ],
    'asks': [
        {'price': '0.56', 'size': '1500'},
        {'price': '0.57', 'size': '1800'},
        ...
    ]
}
```

### 常见策略模式

#### 1. 均值回归策略

```python
def mean_reversion_strategy(engine, tick):
    up_price = float(tick['up_orderbook']['asks'][0]['price'])
    down_price = float(tick['down_orderbook']['asks'][0]['price'])
    
    # 当UP被严重低估时买入
    if up_price < 0.35 and not engine.get_position(Side.UP):
        engine.place_order(Side.UP, size=100.0)
    
    # 当价格回归时止盈
    position = engine.get_position(Side.UP)
    if position and up_price > 0.50:
        engine.close_position(Side.UP)
```

#### 2. 动量策略

```python
def momentum_strategy(engine, tick):
    # 存储历史价格（需要在函数外部维护）
    if not hasattr(momentum_strategy, 'price_history'):
        momentum_strategy.price_history = []
    
    up_price = float(tick['up_orderbook']['asks'][0]['price'])
    momentum_strategy.price_history.append(up_price)
    
    # 保留最近20个价格
    if len(momentum_strategy.price_history) > 20:
        momentum_strategy.price_history.pop(0)
    
    # 价格上涨趋势时买入
    if len(momentum_strategy.price_history) >= 20:
        recent_trend = momentum_strategy.price_history[-1] - momentum_strategy.price_history[-20]
        if recent_trend > 0.05 and not engine.get_position(Side.UP):
            engine.place_order(Side.UP, size=100.0)
```

#### 3. 套利策略

```python
def arbitrage_strategy(engine, tick):
    up_price = float(tick['up_orderbook']['asks'][0]['price'])
    down_price = float(tick['down_orderbook']['asks'][0]['price'])
    
    # UP + DOWN 应该约等于 1.0
    total = up_price + down_price
    
    # 如果总和明显小于1.0，说明有套利机会
    if total < 0.95:
        engine.place_order(Side.UP, size=50.0)
        engine.place_order(Side.DOWN, size=50.0)
```

## 注意事项

1. **市场自动结算**：每个15分钟市场结束时，引擎会自动平掉所有持仓
2. **资金管理**：下单前会检查资金是否充足
3. **数据一致性**：如果UP和DOWN数据长度不一致，会使用较短的长度
4. **订单执行**：市价单立即执行，限价单在价格合适时执行
5. **手续费**：当前设置为0，可根据需要调整

## 回测结果

回测完成后会显示：
- 初始资金 vs 最终资金
- 总盈亏和收益率
- 总交易次数
- 盈利/亏损交易数
- 胜率

## 扩展开发

### 自定义策略

在 `strategy/` 目录下创建新的策略文件：

```python
# strategy/my_strategy.py
from datetime import datetime
from dataloader import DataLoader
from engine import BacktestEngine, Side, OrderType

def my_custom_strategy(engine, tick):
    # 你的策略逻辑
    pass

if __name__ == "__main__":
    loader = DataLoader(start_time, end_time)
    engine = BacktestEngine(loader, initial_balance=1000.0)
    engine.set_strategy(my_custom_strategy)
    engine.run()
```

### 添加指标

可以在策略函数中维护自定义指标：

```python
def strategy_with_indicators(engine, tick):
    if not hasattr(strategy_with_indicators, 'sma'):
        strategy_with_indicators.sma = []
    
    # 计算移动平均
    price = float(tick['up_orderbook']['asks'][0]['price'])
    strategy_with_indicators.sma.append(price)
    
    if len(strategy_with_indicators.sma) > 50:
        strategy_with_indicators.sma.pop(0)
        avg = sum(strategy_with_indicators.sma) / len(strategy_with_indicators.sma)
        
        # 使用均线信号
        if price < avg * 0.95:
            engine.place_order(Side.UP, size=100.0)
```

## 性能优化

- 使用 `tick_index % N == 0` 来控制策略执行频率，避免每个tick都执行
- 及时平仓不需要的持仓，释放资金
- 使用限价单而非市价单以获得更好的价格

## 故障排查

1. **"找到 0 个市场"**：检查时间范围和数据目录路径
2. **IndexError**：已修复，使用最小数据长度
3. **资金不足**：减少订单大小或增加初始资金

