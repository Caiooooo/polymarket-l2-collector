# cache the data until size limit or event ends
import os
import gc
import time
import pandas as pd
from logger_config import setup_logger

logger = setup_logger('file_cache')

trade_limit = 50     # 降低缓存阈值减少内存占用
book_limit = 30      # orderbook 含 bids/asks 数组，内存占用大

# save book to data/1h/btc/orderbooks/1765436400down.parquet
# save trades to data/1h/btc/trades/1765436400down.parquet

# 使用字典存储每个市场的缓存，key 格式: "interval/coin/type/direction/timestamp"
# 例如: "15m/btc/trades/up/1765436400" 或 "1h/eth/orderbooks/down/1765436400"
trades_cache_dict = {}
orderbook_cache_dict = {}

# 缓存的窗口数量上限（超出后清理最旧的窗口）
MAX_CACHED_WINDOWS = 30      # 低内存机器降低窗口缓存数


def cleanup_old_cache(cache_dict, max_windows=MAX_CACHED_WINDOWS):
    """清理超出上限的旧窗口缓存，防止内存无限增长"""
    if len(cache_dict) <= max_windows:
        return
    # 按时间戳排序，删除最旧的
    sorted_keys = sorted(cache_dict.keys(), key=lambda k: int(k.split('/')[-1]) if k.split('/')[-1].isdigit() else 0)
    keys_to_remove = sorted_keys[:len(sorted_keys) - max_windows]
    for key in keys_to_remove:
        del cache_dict[key]
    if keys_to_remove:
        logger.debug(f"清理了 {len(keys_to_remove)} 个旧缓存窗口 (剩余 {len(cache_dict)})")


def optimize_data_for_parquet(data):
    """优化数据以提高 parquet 压缩率

    将 price 和 size 乘以 100 存储为整数
    将 price/size 键名精简为 p/s
    timestamp 保持为 long (int)
    """
    if not data:
        return data

    def convert_order_item(item):
        """转换订单项中的 price/size 为 p/s"""
        if not isinstance(item, dict):
            return item

        converted = {}
        for key, value in item.items():
            if key == 'price' and value is not None and not pd.isna(value):
                converted['p'] = int(float(value) * 100)
            elif key == 'size' and value is not None and not pd.isna(value):
                converted['s'] = int(float(value) * 100)
            else:
                converted[key] = value
        return converted

    optimized_data = []
    for record in data:
        optimized_record = {}

        for key, value in record.items():
            # 处理 bids 和 asks 列表
            if key in ['bids', 'asks'] and isinstance(value, list):
                optimized_record[key] = [
                    convert_order_item(item) for item in value]
            # 处理顶层的 price (转为 p)
            elif key == 'price' and value is not None and not pd.isna(value):
                optimized_record['p'] = int(float(value) * 100)
            # 处理顶层的 size (转为 s)
            elif key == 'size' and value is not None and not pd.isna(value):
                optimized_record['s'] = int(float(value) * 100)
            # timestamp 确保是整数
            elif key == 'timestamp' and value is not None and not pd.isna(value):
                optimized_record[key] = int(value)
            # 其他字段保持不变
            else:
                optimized_record[key] = value

        optimized_data.append(optimized_record)

    return optimized_data


def restore_data_from_parquet(data):
    """从 parquet 恢复数据

    将 p/s 恢复为 price/size 并除以 100 恢复为浮点数
    """
    if not data:
        return data

    def restore_order_item(item):
        """恢复订单项中的 p/s 为 price/size"""
        if not isinstance(item, dict):
            return item

        restored = {}
        for key, value in item.items():
            if key == 'p' and value is not None and not pd.isna(value):
                restored['price'] = float(value) / 100
            elif key == 's' and value is not None and not pd.isna(value):
                restored['size'] = float(value) / 100
            # 向后兼容：处理旧格式的 price/size
            elif key == 'price' and value is not None and not pd.isna(value):
                restored['price'] = float(value) / 100
            elif key == 'size' and value is not None and not pd.isna(value):
                restored['size'] = float(value) / 100
            else:
                restored[key] = value
        return restored

    restored_data = []
    for record in data:
        restored_record = {}

        for key, value in record.items():
            # 处理 bids 和 asks 列表
            if key in ['bids', 'asks'] and isinstance(value, list):
                restored_record[key] = [
                    restore_order_item(item) for item in value]
            # 处理顶层的 p (恢复为 price)
            elif key == 'p' and value is not None and not pd.isna(value):
                restored_record['price'] = float(value) / 100
            # 处理顶层的 s (恢复为 size)
            elif key == 's' and value is not None and not pd.isna(value):
                restored_record['size'] = float(value) / 100
            # 向后兼容：处理旧格式的 price/size
            elif key == 'price' and value is not None and not pd.isna(value):
                restored_record['price'] = float(value) / 100
            elif key == 'size' and value is not None and not pd.isna(value):
                restored_record['size'] = float(value) / 100
            # 其他字段保持不变
            else:
                restored_record[key] = value

        restored_data.append(restored_record)

    return restored_data


def get_market_key(file_path):
    """从文件路径提取市场标识符和窗口时间戳"""
    # file_path 格式: data/15m/btc/trades/1765436400up.parquet
    parts = file_path.split("/")
    interval = parts[1]  # 15m 或 1h
    coin = parts[2]      # btc, eth, sol, xrp
    data_type = parts[3]  # trades 或 orderbooks

    # 提取时间戳和方向
    filename = parts[4].split(".")[0]  # 1765436400up 或 1765436400down
    direction = "up" if "up" in filename else "down"
    timestamp_str = filename.replace("up", "").replace("down", "")
    timestamp = int(timestamp_str)

    market_key = f"{interval}/{coin}/{data_type}/{direction}"

    return market_key, timestamp


def get_window_cache_key(file_path):
    """按完整文件路径窗口生成缓存 key，避免不同时间窗共用同一份缓存。"""
    market_key, timestamp = get_market_key(file_path)
    return f"{market_key}/{timestamp}"


def save_trades(data, file_path):
    global trades_cache_dict
    cache_key = get_window_cache_key(file_path)

    # 初始化该市场的缓存（如果不存在）
    if cache_key not in trades_cache_dict:
        trades_cache_dict[cache_key] = {
            'data': []
        }
        # 新窗口加入时清理一次旧缓存
        cleanup_old_cache(trades_cache_dict)

    cache_info = trades_cache_dict[cache_key]

    # 追加新数据
    cache_info['data'].extend(data)

    # 如果达到缓存限制，立即保存
    if len(cache_info['data']) >= trade_limit:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 读取现有数据
        existing_data = []
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                existing_data = restore_data_from_parquet(
                    df.to_dict('records'))
            except (FileNotFoundError, Exception):
                existing_data = []

        # 合并现有数据和缓存数据
        existing_data.extend(cache_info['data'])

        # 优化并保存合并后的数据
        optimized_data = optimize_data_for_parquet(existing_data)
        df = pd.DataFrame(optimized_data)
        df.to_parquet(file_path, index=False,
                      engine='pyarrow', compression='zstd')
        # 显式释放 DataFrame 内存
        del df
        del optimized_data
        del existing_data
        gc.collect()
        # 清空缓存
        cache_info['data'] = []


def save_book(data, file_path):
    global orderbook_cache_dict

    cache_key = get_window_cache_key(file_path)

    # 初始化该市场的缓存（如果不存在）
    if cache_key not in orderbook_cache_dict:
        orderbook_cache_dict[cache_key] = {
            'data': []
        }
        # 新窗口加入时清理一次旧缓存
        cleanup_old_cache(orderbook_cache_dict)

    cache_info = orderbook_cache_dict[cache_key]

    # 追加新数据
    cache_info['data'].extend(data)

    # 如果达到缓存限制，立即保存
    if len(cache_info['data']) >= book_limit:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # 读取现有数据
        existing_data = []
        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                existing_data = restore_data_from_parquet(
                    df.to_dict('records'))
            except (FileNotFoundError, Exception):
                existing_data = []

        # 合并现有数据和缓存数据
        existing_data.extend(cache_info['data'])

        # 优化并保存合并后的数据
        optimized_data = optimize_data_for_parquet(existing_data)
        df = pd.DataFrame(optimized_data)
        df.to_parquet(file_path, index=False,
                      engine='pyarrow', compression='zstd')
        # 显式释放 DataFrame 内存
        del df
        del optimized_data
        del existing_data
        gc.collect()
        # 清空缓存
        cache_info['data'] = []
