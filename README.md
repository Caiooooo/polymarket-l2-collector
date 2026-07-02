# Polymarket L2 Collector

实时采集 Polymarket 上 BTC/ETH 的 L2 orderbook 和 trade 数据，按 5m/15m 时间窗口输出为 Parquet 文件。

## 当前能力

- ✅ **BTC/ETH + SOL/XRP** — 订单簿 (orderbook) + 成交 (trade) 实时数据
- ✅ **5m / 15m** — 两种时间窗口并行采集
- ✅ **Up/Down token 方向** — 可配 DIRECTIONS=up,down 采集完整盘口
- ✅ **WebSocket 实时订阅** — Polymarket CLOB WS channel
- ✅ **REST 快照补采** — `uv run polymarket-backfill` 自动检测数据断档并补采
- ✅ **Parquet 输出** — 按 `data/{interval}/{coin}/{orderbooks|trades}/{timestamp}{direction}.parquet` 结构
- ✅ **币安价格同步** — BTC/ETH/SOL/XRP midprice，供回测对齐
- ✅ **健康监控 + 自动重启** — 内存阈值守卫、WS 断线检测、每日定时重启
- ✅ **原子 Parquet 写入** — 临时文件 + os.replace，避免写坏文件
- ✅ **窗口元数据追踪** — 每个 Parquet 窗口附带 .meta.json（消息数、时间范围、状态）
- ✅ **数据质量检查 CLI** — `uv run polymarket-check-quality`（含窗口断档检测）
- ✅ **Docker 部署** — Dockerfile + docker-compose.yml
- ✅ **CI** — GitHub Actions：ruff + pytest + 导入检查（Python 3.10-3.12）
- ✅ **结构化日志** — 支持 JSON 格式（LOG_FORMAT=json）
- ✅ **87 个测试** — 覆盖窗口计算、slug 生成、资产解析、消息格式化、Parquet 管道、元数据、断档检测、REST 补采
- ✅ **Deprecation warnings** — poly_ws_5min.py / poly_ws_15min.py 指向新 Collector

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

# 5. （可选）Docker 部署
docker compose up -d
```

## 项目结构

```
polymarket-l2-collector/
├── pyproject.toml              # 项目元数据和依赖
├── uv.lock                     # 锁定依赖版本
├── .env.example                # 配置模板
├── README.md
├── polymarket_l2_collector/    # 主 Python 包
│   ├── __init__.py / __main__.py
│   ├── main.py                 # 入口编排（健康监控 + 内存守卫 + 每日重启）
│   ├── config.py               # 配置加载（.env + 默认值）
│   ├── collector.py            # 参数化采集核心
│   ├── market_discovery.py     # Gamma API 市场发现
│   ├── ws_client.py            # WebSocket 连接、订阅、消息接收
│   ├── data_formatter.py       # 消息格式化（orderbook / trade）
│   ├── file_cache.py           # Parquet 写入缓存（原子写入 + 追加刷新）
│   ├── binance_price.py        # 币安 bookTicker 中间价
│   ├── get_asset_id.py         # Gamma API HTTP 客户端（async + sync）
│   ├── window_metadata.py      # 窗口质量元数据 + 数据质量扫描
│   ├── check_quality.py        # 数据质量检查 CLI
│   └── logger_config.py        # 日志配置（plain / JSON）
├── tests/                      # 56 个测试
│   ├── test_collector.py
│   ├── test_file_cache.py
│   ├── test_market_discovery.py
│   ├── test_ws_client.py
│   ├── test_window_metadata.py
│   ├── test_check_quality.py
│   └── test_smoke.py
├── data/                       # Parquet 输出目录（自动创建）
├── Dockerfile + docker-compose.yml
└── .github/workflows/ci.yml
```

## 未来计划

> 以下暂无实现计划，PR 欢迎

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
