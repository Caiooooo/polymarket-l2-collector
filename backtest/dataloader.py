#!/usr/bin/env python3
"""
DataLoader - 回测数据加载器
用于加载 Polymarket 15分钟 BTC up/down 市场的 orderbook 数据
"""
import os
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class DataLoader:
    """
    回测数据加载器
    负责按时间顺序加载 15 分钟市场的 orderbook 数据
    """

    def __init__(self,
                 start_time: datetime,
                 end_time: datetime,
                 data_dir: str = "/root/poly/data/15m/btc/orderbooks",
                 interval_minutes: int = 15,
                 cache_size: int = 30):
        """
        初始化数据加载器

        Args:
            start_time: 回测开始时间
            end_time: 回测结束时间
            data_dir: orderbook 数据目录
            interval_minutes: 市场间隔（分钟），默认15分钟
            cache_size: 缓存大小，每次批量加载的市场数量，默认10个
        """
        self.start_time = start_time
        self.end_time = end_time
        self.data_dir = Path(data_dir)
        self.interval_minutes = interval_minutes

        # 当前状态
        self.current_market_time = None
        self.current_market_data = None  # {up: [...], down: [...]}
        self.current_tick_index = 0

        # 获取所有可用的市场时间戳
        self.available_markets = self._load_available_markets()
        self.market_index = 0

        # 缓存机制
        self.cache_size = cache_size  # 每次批量加载的市场数量
        self.cache_threshold = 3  # 当缓存剩余数量低于此值时，触发新的批量加载
        self.market_cache = []  # 缓存的市场数据列表
        self.cache_start_index = 0  # 缓存对应的市场索引起始位置

        # print(f"[DataLoader] 初始化完成")
        # print(f"[DataLoader] 时间范围: {start_time} 到 {end_time}")
        # print(f"[DataLoader] 找到 {len(self.available_markets)} 个市场")
        # print(f"[DataLoader] 缓存配置: 批量加载 {cache_size} 个市场")

    def _load_available_markets(self) -> List[int]:
        """
        扫描数据目录，找到所有可用的市场时间戳

        Returns:
            排序后的时间戳列表
        """
        timestamps = set()

        if not self.data_dir.exists():
            raise FileNotFoundError(f"数据目录不存在: {self.data_dir}")

        # 扫描所有 up parquet 文件
        for file_path in self.data_dir.glob("*up.parquet"):
            timestamp = int(file_path.stem.replace("up", ""))
            file_time = datetime.fromtimestamp(timestamp)

            # 检查是否在时间范围内
            if self.start_time <= file_time <= self.end_time:
                # 确保 down 文件也存在
                down_file = self.data_dir / f"{timestamp}down.parquet"
                if down_file.exists():
                    timestamps.add(timestamp)

        return sorted(list(timestamps))

    def _read_parquet_file(self, file_path: Path) -> pd.DataFrame:
        """同步读取 parquet 文件的辅助函数"""
        return pd.read_parquet(file_path, engine='pyarrow')

    async def _load_market_data_async(self, timestamp: int) -> Optional[Dict]:
        """
        异步加载单个市场的数据

        Args:
            timestamp: 市场时间戳

        Returns:
            市场数据字典，如果加载失败返回 None
        """
        up_file = self.data_dir / f"{timestamp}up.parquet"
        down_file = self.data_dir / f"{timestamp}down.parquet"

        try:
            # 并发读取 up 和 down 文件
            loop = asyncio.get_event_loop()
            up_df, down_df = await asyncio.gather(
                loop.run_in_executor(None, self._read_parquet_file, up_file),
                loop.run_in_executor(None, self._read_parquet_file, down_file)
            )

            # 恢复数据：price 和 size 除以 100
            if 'price' in up_df.columns:
                up_df['price'] = up_df['price'] / 100.0
            if 'size' in up_df.columns:
                up_df['size'] = up_df['size'] / 100.0
            up_data = up_df.to_dict('records')

            if 'price' in down_df.columns:
                down_df['price'] = down_df['price'] / 100.0
            if 'size' in down_df.columns:
                down_df['size'] = down_df['size'] / 100.0
            down_data = down_df.to_dict('records')

            # 使用最小长度以确保数据一致性
            min_len = min(len(up_data), len(down_data))
            market_datetime = datetime.fromtimestamp(timestamp)

            market_info = {
                'timestamp': timestamp,
                'datetime': market_datetime,
                'up': up_data,
                'down': down_data,
                'total_ticks': min_len
            }

            return market_info

        except Exception as e:
            print(f"[DataLoader] 加载市场失败 {timestamp}: {e}")
            return None

    def _load_market_data(self, timestamp: int) -> Optional[Dict]:
        """
        加载单个市场的数据（同步版本，用于向后兼容）

        Args:
            timestamp: 市场时间戳

        Returns:
            市场数据字典，如果加载失败返回 None
        """
        up_file = self.data_dir / f"{timestamp}up.parquet"
        down_file = self.data_dir / f"{timestamp}down.parquet"

        try:
            # 读取 up 数据
            df = pd.read_parquet(up_file, engine='pyarrow')
            # 恢复数据：price 和 size 除以 100
            if 'price' in df.columns:
                df['price'] = df['price'] / 100.0
            if 'size' in df.columns:
                df['size'] = df['size'] / 100.0
            up_data = df.to_dict('records')

            # 读取 down 数据
            df = pd.read_parquet(down_file, engine='pyarrow')
            # 恢复数据：price 和 size 除以 100
            if 'price' in df.columns:
                df['price'] = df['price'] / 100.0
            if 'size' in df.columns:
                df['size'] = df['size'] / 100.0
            down_data = df.to_dict('records')

            # 使用最小长度以确保数据一致性
            min_len = min(len(up_data), len(down_data))
            market_datetime = datetime.fromtimestamp(timestamp)

            market_info = {
                'timestamp': timestamp,
                'datetime': market_datetime,
                'up': up_data,
                'down': down_data,
                'total_ticks': min_len
            }

            return market_info

        except Exception as e:
            print(f"[DataLoader] 加载市场失败 {timestamp}: {e}")
            return None

    async def _load_markets_batch_async(self, start_index: int, count: int) -> List[Dict]:
        """
        异步批量加载市场数据到缓存（并发读取多个文件）

        Args:
            start_index: 开始的市场索引
            count: 要加载的市场数量

        Returns:
            加载成功的市场数据列表
        """
        end_index = min(start_index + count, len(self.available_markets))
        timestamps = [self.available_markets[i]
                      for i in range(start_index, end_index)]

        # 并发加载所有市场数据
        tasks = [self._load_market_data_async(ts) for ts in timestamps]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 过滤掉失败的结果
        loaded_markets = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[DataLoader] 加载市场失败 {timestamps[i]}: {result}")
            elif result is not None:
                loaded_markets.append(result)

        return loaded_markets

    def _load_markets_batch(self, start_index: int, count: int) -> List[Dict]:
        """
        批量加载市场数据到缓存（使用异步并发读取）

        Args:
            start_index: 开始的市场索引
            count: 要加载的市场数量

        Returns:
            加载成功的市场数据列表
        """
        # 使用异步方法并发加载
        try:
            # 尝试获取当前事件循环
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果事件循环正在运行，使用同步方式（避免嵌套事件循环问题）
                return self._load_markets_batch_sync(start_index, count)
        except RuntimeError:
            # 没有事件循环，可以创建新的
            pass

        # 运行异步加载
        try:
            return asyncio.run(self._load_markets_batch_async(start_index, count))
        except RuntimeError:
            # 如果无法创建事件循环，回退到同步方式
            return self._load_markets_batch_sync(start_index, count)

    def _load_markets_batch_sync(self, start_index: int, count: int) -> List[Dict]:
        """
        同步批量加载市场数据（备用方法）

        Args:
            start_index: 开始的市场索引
            count: 要加载的市场数量

        Returns:
            加载成功的市场数据列表
        """
        loaded_markets = []
        end_index = min(start_index + count, len(self.available_markets))

        for i in range(start_index, end_index):
            timestamp = self.available_markets[i]
            market_data = self._load_market_data(timestamp)
            if market_data:
                loaded_markets.append(market_data)

        return loaded_markets

    def _ensure_cache(self):
        """确保缓存中有足够的数据"""
        # 计算当前市场在缓存中的位置
        cache_pos = self.market_index - self.cache_start_index

        # 如果缓存不足，批量加载更多
        if cache_pos >= len(self.market_cache) - self.cache_threshold:
            # 计算需要加载的起始位置
            new_start_index = self.market_index
            # 批量加载
            new_markets = self._load_markets_batch(
                new_start_index, self.cache_size)
            if new_markets:
                # 更新缓存
                self.market_cache = new_markets
                self.cache_start_index = new_start_index
                # print(
                #     f"[DataLoader] 批量加载 {len(new_markets)} 个市场到缓存 (索引 {new_start_index})")

    def has_next_market(self) -> bool:
        """
        检查是否还有下一个市场

        Returns:
            True 如果还有市场，否则 False
        """
        return self.market_index < len(self.available_markets)

    def next_market(self) -> Optional[Dict]:
        """
        加载下一个 15 分钟市场的所有数据（优先从缓存读取）

        Returns:
            市场数据字典，包含:
            {
                'timestamp': 市场开始时间戳,
                'datetime': 市场开始时间,
                'up': [orderbook快照列表],
                'down': [orderbook快照列表],
                'total_ticks': tick总数
            }
            如果没有更多市场，返回 None
        """
        if not self.has_next_market():
            return None

        # 确保缓存中有足够的数据
        self._ensure_cache()

        # 从缓存中获取市场数据
        cache_pos = self.market_index - self.cache_start_index
        if cache_pos < len(self.market_cache):
            # 从缓存读取
            market_info = self.market_cache[cache_pos]
        else:
            # 缓存中没有，直接加载（这种情况应该很少发生）
            timestamp = self.available_markets[self.market_index]
            market_info = self._load_market_data(timestamp)
            if not market_info:
                # 加载失败，尝试下一个市场
                self.market_index += 1
                return self.next_market()

        # 更新当前市场状态
        self.current_market_time = market_info['timestamp']
        self.current_market_data = {
            'up': market_info['up'],
            'down': market_info['down']
        }
        self.current_tick_index = 0

        # 移动到下一个市场索引
        self.market_index += 1

        return market_info

    def has_next_tick(self) -> bool:
        """
        检查当前市场是否还有下一个 tick

        Returns:
            True 如果还有 tick，否则 False
        """
        if self.current_market_data is None:
            return False

        # 使用 up 和 down 数据的最小长度
        min_len = min(len(self.current_market_data['up']),
                      len(self.current_market_data['down']))
        return self.current_tick_index < min_len

    def next_tick(self) -> Optional[Dict]:
        """
        加载当前市场的下一个 tick orderbook

        Returns:
            tick 数据字典，包含:
            {
                'market_time': 市场开始时间,
                'tick_index': 当前tick索引,
                'up_orderbook': {bids: [...], asks: [...]},
                'down_orderbook': {bids: [...], asks: [...]}
            }
            如果没有更多 tick，返回 None
        """
        if not self.has_next_tick():
            return None

        tick_idx = self.current_tick_index
        self.current_tick_index += 1

        up_orderbook = self.current_market_data['up'][tick_idx]
        down_orderbook = self.current_market_data['down'][tick_idx]

        tick_data = {
            'market_time': datetime.fromtimestamp(self.current_market_time),
            'tick_index': tick_idx,
            'up_orderbook': up_orderbook,
            'down_orderbook': down_orderbook
        }

        return tick_data

    def get_current_market_time(self) -> Optional[datetime]:
        """
        获取当前市场时间

        Returns:
            当前市场的开始时间，如果没有加载市场则返回 None
        """
        if self.current_market_time is None:
            return None
        return datetime.fromtimestamp(self.current_market_time)

    def get_progress(self) -> Dict:
        """
        获取回测进度信息

        Returns:
            进度字典，包含市场进度和tick进度
        """
        total_markets = len(self.available_markets)
        current_market = self.market_index

        if self.current_market_data:
            total_ticks = min(len(self.current_market_data['up']),
                              len(self.current_market_data['down']))
            current_tick = self.current_tick_index
        else:
            total_ticks = 0
            current_tick = 0

        return {
            'market_progress': f"{current_market}/{total_markets}",
            'market_percentage': (current_market / total_markets * 100) if total_markets > 0 else 0,
            'tick_progress': f"{current_tick}/{total_ticks}",
            'tick_percentage': (current_tick / total_ticks * 100) if total_ticks > 0 else 0
        }


if __name__ == "__main__":
    # 测试代码
    from datetime import datetime

    # 设置测试时间范围
    start = datetime(2024, 12, 11, 11, 0)
    end = datetime(2024, 12, 11, 14, 0)

    loader = DataLoader(start, end)

    # 测试加载第一个市场
    market = loader.next_market()
    if market:
        print(f"\n市场信息:")
        print(f"  时间: {market['datetime']}")
        print(f"  总 ticks: {market['total_ticks']}")

        # 测试加载前3个 ticks
        for i in range(3):
            tick = loader.next_tick()
            if tick:
                print(f"\nTick {i}:")
                print(
                    f"  Up orderbook bids: {len(tick['up_orderbook']['bids'])}")
                print(
                    f"  Down orderbook bids: {len(tick['down_orderbook']['bids'])}")
                print(f"  Up best bid: {tick['up_orderbook']['bids'][0]}")
                print(f"  Down best bid: {tick['down_orderbook']['bids'][0]}")

        # 显示进度
        progress = loader.get_progress()
        print(f"\n进度: {progress}")
