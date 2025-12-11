#!/usr/bin/env python3
"""
从 Polymarket API 响应中提取 asset_id

使用方法:
1. 复制 API 响应的 JSON 数据到文件，然后: python extract_asset_id.py data.json
2. 或者直接从字符串提取: python extract_asset_id.py
"""

import json
import sys


def extract_asset_ids(market_data):
    """从市场数据中提取 asset_id"""

    # 如果是列表，取第一个元素
    if isinstance(market_data, list):
        if not market_data:
            print("空数据")
            return
        market_data = market_data[0]

    # 提取字段
    question = market_data.get('question', 'N/A')
    slug = market_data.get('slug', 'N/A')
    outcomes = market_data.get('outcomes', '[]')
    clob_token_ids = market_data.get('clobTokenIds', '[]')

    # 解析 JSON 字符串
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    # 打印结果
    print("="*80)
    print(f"市场: {question}")
    print(f"Slug: {slug}")
    print("="*80)
    print("\nAsset IDs (用于 WebSocket 订阅):\n")

    for i, (outcome, token_id) in enumerate(zip(outcomes, clob_token_ids)):
        print(f'{outcome:6s} = "{token_id}"')

    print("\n可复制的代码片段：\n")
    print("ASSET_IDS = [")
    for outcome, token_id in zip(outcomes, clob_token_ids):
        print(f'    "{token_id}",  # {outcome}')
    print("]")


def main():
    if len(sys.argv) > 1:
        # 从文件读取
        filename = sys.argv[1]
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
            extract_asset_ids(data)
        except Exception as e:
            print(f"读取文件失败: {e}")
    else:
        # 从标准输入读取
        print("请粘贴 JSON 数据（粘贴后按 Ctrl+D 结束）:")
        try:
            data = json.loads(sys.stdin.read())
            extract_asset_ids(data)
        except Exception as e:
            print(f"解析 JSON 失败: {e}")


if __name__ == "__main__":
    main()
