#!/usr/bin/env python3
"""
获取 Polymarket 市场的 asset_id (token_id)

使用方法:
1. 通过市场 slug: python get_asset_id.py btc-updown-15m-1765359900
2. 通过完整 URL: python get_asset_id.py https://polymarket.com/event/btc-updown-15m-1765359900
3. 搜索市场: python get_asset_id.py --search "BTC"
"""

import sys
import json
import asyncio
import aiohttp
import requests  # 仅 CLI 模式使用


# ---- 异步版本（供 WebSocket 采集器调用，不阻塞事件循环） ----

async def get_market_info_by_slug_async(slug, session=None):
    """通过 slug 异步获取市场信息"""
    if slug.startswith('http'):
        slug = slug.split('/')[-1]

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    close_session = session is None
    if close_session:
        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15))

    try:
        async with session.get(url) as response:
            if response.status != 200:
                return None
            data = await response.json()

            if not data:
                return None

            markets = data[0].get('markets', [])
            # 预处理 JSON 字符串字段
            for market in markets:
                for field in ('outcomes', 'outcomePrices', 'clobTokenIds'):
                    val = market.get(field)
                    if isinstance(val, str):
                        try:
                            market[field] = json.loads(val)
                        except (json.JSONDecodeError, TypeError):
                            pass
            return markets
    except Exception:
        return None
    finally:
        if close_session:
            await session.close()


async def get_asset_id_async(url, session=None):
    """通过 url 异步获取 asset id"""
    return await get_market_info_by_slug_async(url, session)


# ---- 同步版本（向后兼容 CLI 调用） ----

def get_market_info_by_slug(slug):
    """通过 slug 获取市场信息（同步版本，仅供 CLI 使用）"""
    if slug.startswith('http'):
        slug = slug.split('/')[-1]

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"未找到市场: {slug}")
            return None

        event = data[0]
        markets = event.get('markets', [])
        if not markets:
            print("该事件没有市场数据")
            return None

        for market in markets:
            for field in ('outcomes', 'outcomePrices', 'clobTokenIds'):
                val = market.get(field)
                if isinstance(val, str):
                    try:
                        market[field] = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        pass

        return markets

    except requests.exceptions.RequestException as e:
        print(f"API 请求失败: {e}")
        return None


def get_asset_id(url):
    """通过 url 获取 asset id（同步，CLI 用）"""
    return get_market_info_by_slug(url)


def search_markets(keyword):
    """搜索市场"""
    print(f"搜索关键词: {keyword}")

    url = f"https://gamma-api.polymarket.com/events?limit=10"

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        events = response.json()

        matching = []
        for event in events:
            if keyword.lower() in event.get('title', '').lower():
                matching.append(event)

        if not matching:
            print(f"未找到包含 '{keyword}' 的市场")
            return

        print(f"\n找到 {len(matching)} 个相关市场:\n")

        for idx, event in enumerate(matching, 1):
            print(f"【{idx}】 {event.get('title', 'N/A')}")
            print(f"    Slug: {event.get('slug', 'N/A')}")
            print(
                f"    URL: https://polymarket.com/event/{event.get('slug', 'N/A')}")

            markets = event.get('markets', [])
            if markets:
                for market in markets[:2]:
                    token_ids = market.get('clobTokenIds', [])
                    if token_ids:
                        print(f"    Token ID: {token_ids[0]}")
            print()

    except requests.exceptions.RequestException as e:
        print(f"API 请求失败: {e}")


def main():
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  1. 通过 slug: python get_asset_id.py btc-updown-15m-1765359900")
        print("  2. 通过 URL: python get_asset_id.py https://polymarket.com/event/btc-updown-15m-1765359900")
        print("  3. 搜索市场: python get_asset_id.py --search BTC")
        sys.exit(1)

    if sys.argv[1] == '--search':
        if len(sys.argv) < 3:
            print("请提供搜索关键词")
            sys.exit(1)
        search_markets(sys.argv[2])
    else:
        get_market_info_by_slug(sys.argv[1])


if __name__ == "__main__":
    main()
