"""
Unit tests for market discovery — event slug generation and asset ID parsing.
"""

from polymarket_l2_collector.market_discovery import (
    _build_event_slug,
    parse_assets_from_markets,
)


class TestEventSlug:
    """Event slug generation."""

    def test_btc_5m_slug(self):
        slug = _build_event_slug("btc", "5m", 1765359900)
        assert slug == "btc-updown-5m-1765359900"

    def test_eth_15m_slug(self):
        slug = _build_event_slug("eth", "15m", 1765359900)
        assert slug == "eth-updown-15m-1765359900"

    def test_btc_5m_different_ts(self):
        slug = _build_event_slug("btc", "5m", 1765360200)
        assert slug == "btc-updown-5m-1765360200"

    def test_eth_15m_different_ts(self):
        slug = _build_event_slug("eth", "15m", 1765360800)
        assert slug == "eth-updown-15m-1765360800"


class TestAssetParsing:
    """Parsing Gamma API market responses."""

    def test_parse_up_down(self):
        markets = [
            {
                "question": "BTC > $50k?",
                "clobTokenIds": ["111", "222"],
                "outcomes": ["Up", "Down"],
            }
        ]
        result = parse_assets_from_markets(markets)
        assert result == {"up": "111", "down": "222"}

    def test_parse_up_only(self):
        """If Down outcome is missing, only 'up' is returned."""
        markets = [
            {
                "clobTokenIds": ["111"],
                "outcomes": ["Up"],
            }
        ]
        result = parse_assets_from_markets(markets)
        assert result == {"up": "111"}

    def test_parse_both_up_down(self):
        """Verify both 'up' and 'down' directions are extracted correctly."""
        markets = [
            {
                "question": "BTC > $50k?",
                "clobTokenIds": ["aaa", "bbb"],
                "outcomes": ["Up", "Down"],
            }
        ]
        result = parse_assets_from_markets(markets)
        assert result == {"up": "aaa", "down": "bbb"}

    def test_parse_empty_response(self):
        assert parse_assets_from_markets([]) == {}

    def test_parse_string_fields(self):
        """Gamma API sometimes returns JSON-as-string."""
        markets = [
            {
                "clobTokenIds": '["111","222"]',
                "outcomes": '["Up","Down"]',
            }
        ]
        result = parse_assets_from_markets(markets)
        assert result == {"up": "111", "down": "222"}

    def test_parse_malformed_string(self):
        """Malformed JSON strings should not crash."""
        markets = [
            {
                "clobTokenIds": "not-json",
                "outcomes": "also-bad",
            }
        ]
        result = parse_assets_from_markets(markets)
        assert result == {}

    def test_parse_missing_fields(self):
        markets = [{"question": "No token IDs here"}]
        result = parse_assets_from_markets(markets)
        assert result == {}
