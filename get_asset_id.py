#!/usr/bin/env python3
"""
获取 Polymarket 市场的 asset_id (token_id)

使用方法:
1. 通过市场 slug: python get_asset_id.py btc-updown-15m-1765359900
2. 通过完整 URL: python get_asset_id.py https://polymarket.com/event/btc-updown-15m-1765359900
3. 搜索市场: python get_asset_id.py --search "BTC"
"""

import sys
import requests
import json


def get_market_info_by_slug(slug):
    """通过 slug 获取市场信息"""
    # 从 URL 中提取 slug（如果是完整 URL）
    if slug.startswith('http'):
        slug = slug.split('/')[-1]

    # print(f"正在查询市场: {slug}")

    # Polymarket API 端点
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"未找到市场: {slug}")
            return None

        event = data[0]
        # print("\n" + "="*80)
        # print(f"市场名称: {event.get('title', 'N/A')}")
        # print(f"描述: {event.get('description', 'N/A')}")
        # print(f"Condition ID: {event.get('conditionId', 'N/A')}")
        # print("="*80)

        # 获取市场结果
        markets = event.get('markets', [])
        if not markets:
            print("该事件没有市场数据")
            return None

        # print(f"\n找到 {len(markets)} 个市场:\n")

        for idx, market in enumerate(markets, 1):
            # print(f"【市场 {idx}】")
            # print(f"  问题: {market.get('question', 'N/A')}")
            # print(f"  Condition ID: {market.get('conditionId', 'N/A')}")

            # 获取结果选项 - 处理可能是JSON字符串的情况
            outcomes = market.get('outcomes', [])
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except:
                    outcomes = []

            outcomePrices = market.get('outcomePrices', [])
            if isinstance(outcomePrices, str):
                try:
                    outcomePrices = json.loads(outcomePrices)
                except:
                    outcomePrices = []

            clobTokenIds = market.get('clobTokenIds', [])
            if isinstance(clobTokenIds, str):
                try:
                    clobTokenIds = json.loads(clobTokenIds)
                except:
                    clobTokenIds = []

            if outcomes and clobTokenIds:
                # print(f"  选项:")
                for i, outcome in enumerate(outcomes):
                    price = outcomePrices[i] if i < len(
                        outcomePrices) else 'N/A'
                    token_id = clobTokenIds[i] if i < len(
                        clobTokenIds) else 'N/A'
            #         print(f"    - {outcome}: {price} (Token ID: {token_id})")
            # print()

        return markets

    except requests.exceptions.RequestException as e:
        print(f"API 请求失败: {e}")
        return None


def get_asset_id(url):
    """通过 url 获取 asset id"""
    return get_market_info_by_slug(url)


def search_markets(keyword):
    """搜索市场"""
    print(f"搜索关键词: {keyword}")

    url = f"https://gamma-api.polymarket.com/events?limit=10"

    try:
        response = requests.get(url)
        response.raise_for_status()
        events = response.json()

        # 过滤包含关键词的市场
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
                for market in markets[:2]:  # 只显示前2个
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
