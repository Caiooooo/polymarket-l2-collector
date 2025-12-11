"""
资产获取工具函数
"""
import time
import json
from datetime import datetime
import pytz
from get_asset_id import get_asset_id
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


def get_assets(coin, interval):
    """获取指定币种和时间间隔的市场资产 ID"""
    # 获取当前时间戳
    timestamp = int(time.time())
    gap_time = 15*60
    if interval == "15m":
        gap_time = 15 * 60
    elif interval == "1h":
        gap_time = 60 * 60
    now_opening_market = (timestamp // gap_time) * gap_time
    m_close_time = now_opening_market + gap_time

    # 根据间隔构建 URL
    if interval == "15m":
        url = f"https://polymarket.com/event/{coin}-updown-{interval}-{now_opening_market}"
    elif interval == "1h":
        url = get_1h_url(coin, now_opening_market)

    # 获取市场数据
    data = get_asset_id(url)
    if data:
        market = data[0]
        clob_token_ids_str = market.get('clobTokenIds', '[]')
        clob_token_ids = json.loads(clob_token_ids_str)
        outcomes_str = market.get('outcomes', '[]')
        outcomes = json.loads(outcomes_str)
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
