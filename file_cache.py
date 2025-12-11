# cache the data until size limit or event ends
import os
import json
from logger_config import setup_logger

logger = setup_logger('file_cache')

trade_limit = 100
book_limit = 100

# save book to data/1h/btc/orderbooks/1765436400down.json
# save trades to data/1h/btc/trades/1765436400down.json

# 使用字典存储每个市场的缓存，key 格式: "interval/coin/type/direction"
# 例如: "15m/btc/trades/up" 或 "1h/eth/orderbooks/down"
trades_cache_dict = {}
orderbook_cache_dict = {}


def get_market_key(file_path):
    """从文件路径提取市场标识符"""
    # file_path 格式: data/15m/btc/trades/1765436400up.json
    parts = file_path.split("/")
    interval = parts[1]  # 15m 或 1h
    coin = parts[2]      # btc, eth, sol, xrp
    data_type = parts[3]  # trades 或 orderbooks

    # 提取时间戳和方向
    filename = parts[4].split(".")[0]  # 1765436400up 或 1765436400down
    direction = "up" if "up" in filename else "down"
    timestamp_str = filename.replace("up", "").replace("down", "")
    timestamp = int(timestamp_str)

    # 判断方向

    market_key = f"{interval}/{coin}/{data_type}/{direction}"

    return market_key, timestamp


def save_trades(data, file_path):
    global trades_cache_dict
    # print(data, file_path)
    market_key, timestamp = get_market_key(file_path)

    # 初始化该市场的缓存（如果不存在）
    if market_key not in trades_cache_dict:
        trades_cache_dict[market_key] = {
            'data': [],
            'timestamp': timestamp
        }

    cache_info = trades_cache_dict[market_key]

    # 如果是新的时间窗口，保存之前的缓存并清空
    if cache_info['timestamp'] != timestamp:
        print("new timestamp", timestamp)
        old_timestamp = cache_info['timestamp']
        if cache_info['data']:
            # 构建旧的文件路径
            old_file_path = file_path.replace(
                str(timestamp), str(old_timestamp))
            os.makedirs(os.path.dirname(old_file_path), exist_ok=True)

            # 读取现有数据
            existing_data = []
            if os.path.exists(old_file_path):
                try:
                    with open(old_file_path, 'r') as f:
                        existing_data = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    existing_data = []

            # 合并现有数据和缓存数据
            existing_data.extend(cache_info['data'])

            # 保存合并后的数据
            with open(old_file_path, 'w') as f:
                json.dump(existing_data, f, indent=4)
            logger.info(f"💾 交易已保存: {old_file_path} ({len(existing_data)} 条)")

        # 清空缓存
        cache_info['data'] = []
        cache_info['timestamp'] = timestamp

    # 追加新数据
    cache_info['data'].extend(data)

    # 如果达到缓存限制，立即保存
    if len(cache_info['data']) >= trade_limit:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 读取现有数据
        existing_data = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                existing_data = []

        # 合并现有数据和缓存数据
        existing_data.extend(cache_info['data'])

        # 保存合并后的数据
        with open(file_path, 'w') as f:
            json.dump(existing_data, f, indent=4)
        # logger.info(f"💾 交易已保存(达到限制): {file_path} ({len(existing_data)} 条)")
        # 清空缓存
        cache_info['data'] = []


def save_book(data, file_path):
    global orderbook_cache_dict

    market_key, timestamp = get_market_key(file_path)

    # 初始化该市场的缓存（如果不存在）
    if market_key not in orderbook_cache_dict:
        orderbook_cache_dict[market_key] = {
            'data': [],
            'timestamp': timestamp
        }

    cache_info = orderbook_cache_dict[market_key]

    # 如果是新的时间窗口，保存之前的缓存并清空
    if cache_info['timestamp'] != timestamp:
        old_timestamp = cache_info['timestamp']
        if cache_info['data']:
            # 构建旧的文件路径
            old_file_path = file_path.replace(
                str(timestamp), str(old_timestamp))
            os.makedirs(os.path.dirname(old_file_path), exist_ok=True)

            # 读取现有数据
            existing_data = []
            if os.path.exists(old_file_path):
                try:
                    with open(old_file_path, 'r') as f:
                        existing_data = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    existing_data = []

            # 合并现有数据和缓存数据
            existing_data.extend(cache_info['data'])

            # 保存合并后的数据
            with open(old_file_path, 'w') as f:
                json.dump(existing_data, f, indent=4)
            logger.info(f"💾 订单簿已保存: {old_file_path} ({len(existing_data)} 条)")

        # 清空缓存
        cache_info['data'] = []
        cache_info['timestamp'] = timestamp

    # 追加新数据
    cache_info['data'].extend(data)

    # 如果达到缓存限制，立即保存
    if len(cache_info['data']) >= book_limit:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 读取现有数据
        existing_data = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    existing_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                existing_data = []

        # 合并现有数据和缓存数据
        existing_data.extend(cache_info['data'])

        # 保存合并后的数据
        with open(file_path, 'w') as f:
            json.dump(existing_data, f, indent=4)
        # logger.info(f"💾 订单簿已保存(达到限制): {file_path} ({len(existing_data)} 条)")
        # 清空缓存
        cache_info['data'] = []
