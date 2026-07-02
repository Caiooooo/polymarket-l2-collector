# Polymarket L2 Collector

实时采集 Polymarket 上 BTC/ETH 的 L2 orderbook 和 trade 数据，按 5m/15m 时间窗口输出为 Parquet 文件。

## 当前能力

- ✅ **BTC/ETH** — 订单簿 (orderbook) + 成交 (trade) 实时数据
- ✅ **5m / 15m** — 两种时间窗口并行采集
- ✅ **Up token 方向** — 仅订阅 Up 方向资产（完整盘口需同时采 Up/Down）
- ✅ **WebSocket 实时订阅** — Polymarket CLOB WS channel
- ✅ **Parquet 输出** — 按 `data/{interval}/{coin}/{orderbooks|trades}/{timestamp}{direction}.parquet` 结构
- ✅ **币安价格同步** — 辅助记录 midprice，供回测对齐
- ✅ **健康监控 + 自动重启** — 每日 03:00 自动重启，检测卡死/OOM

## 快速开始

```bash
# 1. 安装 uv（如果没有）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. 安装依赖
uv sync

# 3. 复制配置（可选，有默认值）
cp .env.example .env

# 4. 运行采集
uv run polymarket-l2-collector

# 或后台启动（守护进程模式）
uv run start_bg.sh
```

## 项目结构

```
polymarket-l2-collector/
├── pyproject.toml              # 项目元数据和依赖
├── uv.lock                     # 锁定依赖版本
├── .env.example                # 配置模板
├── README.md
├── polymarket_l2_collector/    # 主 Python 包
│   ├── __init__.py
│   ├── config.py               # 配置加载（.env + 默认值）
│   ├── collector.py            # 参数化采集核心（替代 poly_ws_5m.py / poly_ws_15m.py）
│   ├── market_discovery.py     # Gamma API 市场发现
│   ├── ws_client.py            # WebSocket 连接、订阅、消息接收
│   ├── data_formatter.py       # 消息格式化（orderbook / trade）
│   ├── file_cache.py           # Parquet 写入缓存（逐层刷新 + 原子写入）
│   ├── binance_price.py        # 币安 bookTicker 中间价
│   ├── asset_utils.py          # Gamma API 资产 ID 查询
│   ├── get_asset_id.py         # Gamma API HTTP 客户端
│   ├── logger_config.py        # 日志配置
│   └── main.py                 # 入口编排
├── tests/                      # 测试
│   └── test_collector.py
├── data/                       # Parquet 输出目录（自动创建）
├── daemon.py                   # 独立守护进程
├── start_bg.sh                 # 后台启动脚本
├── stop.sh                     # 停止脚本
└── see.sh                      # 状态查看脚本
```

## 未实现 / 未来计划

> 以下标记为 ❌ 的内容尚未实现，PR 欢迎

- ❌ SOL / XRP 支持（当前仅 BTC/ETH）
- ❌ Down token 方向（当前仅 Up）
- ❌ REST 快照补采
- ❌ 质量检测脚本（空文件、重复时间戳、异常价差）
- ❌ Docker Compose / systemd 部署
- ❌ 分析报表工具

## 数据输出

每条记录包含：
| 字段 | 类型 | 说明 |
|------|------|------|
| `bids/asks` | `list[{p, s}]` | 订单簿买方/卖方 depth，p=price×100, s=size×100 (int) |
| `price/size` | `int` | trade 的 price 和 size（×100 存储） |
| `timestamp` | `string` | Polymarket 消息时间戳 (ms) |
| `local_timestamp` | `string` | 本地接收时间戳 (ms) |
| `asset_price` | `float` | 币安该币种 midprice |
| `window_open_ts` | `int` | 对应时间窗口起始 Unix 秒 |
| `side` | `string` | trade 方向 ("buy" / "sell") |

> Parquet 中 price/size 字段压缩为 `p`/`s` 整数以节省空间。读取后需除以 100 恢复浮点值。

## 参考文档

- [Polymarket CLOB WebSocket 文档](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
- [Polymarket API 文档](https://docs.polymarket.com/)
