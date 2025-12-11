# Polymarket 数据采集器

采集 Polymarket 上 BTC、ETH、SOL、XRP 的 Up or Down 15分钟 Orderbook 数据，用于量化交易回测。

## 特性

- ✅ 自动采集 BTC、ETH、SOL、XRP 的 orderbook 数据
- ✅ 支持 WebSocket 实时订阅
- ✅ 支持 REST API 定时快照
- ✅ 自动保存数据到结构化目录
- ✅ 完整的日志记录
- ✅ 数据分析和报表工具
- ✅ 灵活的市场配置

## 快速开始

### 1. 安装依赖

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置市场

使用 market_finder 工具从 Polymarket URL 获取市场配置：

```bash
python market_finder.py https://polymarket.com/event/btc-updown-15m-1765359900
```


## 项目结构

```
poly/
├── README.md              # 项目说明
├── data/                  # 数据目录
│   ├── btc/              # BTC 数据
│   │   └── 12-10/        # 日期 (MM-DD)
│   │       └── HHMMSS_orderbook.json
│   ├── eth/              # ETH 数据
│   ├── sol/              # SOL 数据
│   └── xrp/              # XRP 数据
└── logs/                  # 日志目录
    └── collector_*.log
```

## 参考文档

- Polymarket 示例: https://polymarket.com/event/btc-updown-15m-1765359900
- WebSocket 文档: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
- API 文档: https://docs.polymarket.com/


Market Channel  市场渠道
Public channel for updates related to market updates (level 2 price data).
市场更新（二级价格数据）的公开渠道。
SUBSCRIBE  订阅
<wss-channel> market
​
Book Message  书信
Emitted When:  发出时：
First subscribed to a market
首次订阅市场
When there is a trade that affects the book
当有影响到这本书的交易时
​
Structure  结构
Name  姓名	Type  类型	Description  描述
event_type  事件类型	string  字符串	”book”  书籍
asset_id  资产编号	string  字符串	asset ID (token ID)  资产 ID（代币 ID）
market  市场	string  字符串	condition ID of market  市场条件 ID
timestamp  时间戳	string  字符串	unix timestamp the current book generation in milliseconds (1/1,000 second)
Unix 时间戳当前书籍生成的毫秒数（1/1,000 秒）
hash  哈希	string  字符串	hash summary of the orderbook content
订单簿内容的哈希摘要
buys  购买	OrderSummary[]  订单摘要[]	list of type (size, price) aggregate book levels for buys
买入的类型（尺寸、价格）汇总的书籍级别列表
sells  销售	OrderSummary[]  订单摘要[]	list of type (size, price) aggregate book levels for sells
销售的类型（尺寸、价格）汇总的书籍级别列表
Where a OrderSummary object is of the form:
一个订单摘要对象的形式是：
Name  姓名	Type  类型	Description  描述
price  价格	string  字符串	size available at that price level
该价格水平下的可用尺寸
size  尺寸	string  字符串	price of the orderbook level
订单簿水平的价格
Response
  回复
  复制
  问 AI
{
  "event_type": "book",
  "asset_id": "65818619657568813474341868652308942079804919287380422192892211131408793125422",
  "market": "0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af",
  "bids": [
    { "price": ".48", "size": "30" },
    { "price": ".49", "size": "20" },
    { "price": ".50", "size": "15" }
  ],
  "asks": [
    { "price": ".52", "size": "25" },
    { "price": ".53", "size": "60" },
    { "price": ".54", "size": "10" }
  ],
  "timestamp": "123456789000",
  "hash": "0x0...."
}
​
