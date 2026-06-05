"""
资产获取工具函数
"""
import time
import json
from datetime import datetime
import pytz
from get_asset_id import get_asset_id_async
from logger_config import setup_logger

logger = setup_logger('asset_utils')


def get_1h_url(coin, now_opening_market):
    """生成 1 小时市场的 URL"""
    # 将UTC时间戳转换为ET时区
    utc_time = datetime.fromtimestamp(now_opening_market, tz=pytz.UTC)
    et_timezone = pytz.timezone('US/Eastern')
    et_time = utc_time.astimezone(et_timezone)

    # 月份名称映射（全小写）
    month_names = {
        1: 'january', 2: 'february', 3: 'march', 4: 'april',
        5: 'may', 6: 'june', 7: 'july', 8: 'august',
        9: 'september', 10: 'october', 11: 'november', 12: 'december'
    }

    coin_name = {
        "btc": "bitcoin",
        "eth": "ethereum",
        "sol": "solana",
        "xrp": "xrp"
    }

    # 格式化日期：month-day
    month = month_names[et_time.month]
    day = et_time.day

    # 格式化时间：12小时制，小写pm/am
    hour_12 = et_time.hour % 12
    if hour_12 == 0:
        hour_12 = 12
    period = 'pm' if et_time.hour >= 12 else 'am'
    time_str = f"{hour_12}{period}"

    # 构建URL
    url = f"https://polymarket.com/event/{coin_name[coin]}-up-or-down-{month}-{day}-{time_str}-et"
    return url


async def get_assets(coin, interval, target_timestamp=None):
    """获取指定币种和时间间隔的市场资产 ID（异步版本）"""
    # 获取当前时间戳
    timestamp = int(target_timestamp) if target_timestamp is not None else int(time.time())
    gap_time = 15 * 60
    if interval == "5m":
        gap_time = 5 * 60
    elif interval == "15m":
        gap_time = 15 * 60
    elif interval == "1h":
        gap_time = 60 * 60
    now_opening_market = (timestamp // gap_time) * gap_time

    # 根据间隔构建 URL
    if interval in ["5m", "15m"]:
        url = f"https://polymarket.com/event/{coin}-updown-{interval}-{now_opening_market}"
    elif interval == "1h":
        url = get_1h_url(coin, now_opening_market)
    else:
        logger.error(f"不支持的 interval: {interval}")
        return {}

    # 获取市场数据（异步，不阻塞事件循环）
    markets = await get_asset_id_async(url)
    if markets:
        market = markets[0]
        clob_token_ids = market.get('clobTokenIds', [])
        outcomes = market.get('outcomes', [])

        # 确保是 list 类型
        if isinstance(clob_token_ids, str):
            try:
                clob_token_ids = json.loads(clob_token_ids)
            except (json.JSONDecodeError, TypeError):
                clob_token_ids = []
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except (json.JSONDecodeError, TypeError):
                outcomes = []

        logger.info("="*80)
        logger.info(f"市场: {market.get('question', 'N/A')} 获取成功")
        logger.info("="*80)

        # 提取 up 和 down 的 asset_id
        up = ""
        down = ""
        for outcome, token_id in zip(outcomes, clob_token_ids):
            if outcome == "Up":
                up = token_id
            if outcome == "Down":
                down = token_id

        return {coin: {"up": up, "down": down}}

    else:
        logger.warning("未能获取市场数据")
        return {}
