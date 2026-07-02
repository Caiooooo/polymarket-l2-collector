"""
Unit tests for Parquet optimisation helpers.
"""

from polymarket_l2_collector.file_cache import (
    _build_file_path,
    _parse_file_path,
    optimize_for_parquet,
    restore_from_parquet,
)


class TestFileCacheKey:
    """File path parsing and building."""

    def test_build_path_5m_btc_orderbook_up(self):
        path = _build_file_path("data", "5m", "btc", "orderbooks", 1765359900, "up")
        assert path == "data/5m/btc/orderbooks/1765359900up.parquet"

    def test_build_path_15m_eth_trades_up(self):
        path = _build_file_path("data", "15m", "eth", "trades", 1765360800, "up")
        assert path == "data/15m/eth/trades/1765360800up.parquet"

    def test_parse_path(self):
        interval, coin, data_type, direction, ts = _parse_file_path("data/5m/btc/orderbooks/1765359900up.parquet")
        assert interval == "5m"
        assert coin == "btc"
        assert data_type == "orderbooks"
        assert direction == "up"
        assert ts == 1765359900

    def test_build_path_down_direction(self):
        path = _build_file_path("data", "5m", "btc", "orderbooks", 1765359900, "down")
        assert path == "data/5m/btc/orderbooks/1765359900down.parquet"

    def test_parse_path_down_direction(self):
        interval, coin, data_type, direction, ts = _parse_file_path("data/5m/btc/orderbooks/1765359900down.parquet")
        assert interval == "5m"
        assert coin == "btc"
        assert data_type == "orderbooks"
        assert direction == "down"
        assert ts == 1765359900

    def test_parse_path_down(self):
        interval, coin, data_type, direction, ts = _parse_file_path("data/15m/eth/trades/1765360800down.parquet")
        assert direction == "down"
        assert ts == 1765360800


class TestOptimisation:
    """Integer ↔ float optimisation helpers."""

    def test_optimize_book_price_size(self):
        raw = [
            {
                "bids": [{"price": "0.48", "size": "30.0"}],
                "asks": [{"price": "0.52", "size": "25.0"}],
                "timestamp": "1765359900123",
            }
        ]
        opt = optimize_for_parquet(raw)
        assert opt[0]["bids"][0]["p"] == 48
        assert opt[0]["bids"][0]["s"] == 3000
        assert opt[0]["asks"][0]["p"] == 52
        assert opt[0]["asks"][0]["s"] == 2500
        assert opt[0]["timestamp"] == 1765359900123

    def test_restore_book_price_size(self):
        opt = [
            {
                "bids": [{"p": 48, "s": 3000}],
                "asks": [{"p": 52, "s": 2500}],
            }
        ]
        restored = restore_from_parquet(opt)
        assert restored[0]["bids"][0]["price"] == 0.48
        assert restored[0]["bids"][0]["size"] == 30.0
        assert restored[0]["asks"][0]["price"] == 0.52
        assert restored[0]["asks"][0]["size"] == 25.0

    def test_optimize_trade_price_size(self):
        raw = [{"price": "0.50", "size": "100.0", "side": "buy"}]
        opt = optimize_for_parquet(raw)
        assert opt[0]["p"] == 50
        assert opt[0]["s"] == 10000
        assert opt[0]["side"] == "buy"

    def test_restore_trade_price_size(self):
        opt = [{"p": 50, "s": 10000, "side": "buy"}]
        restored = restore_from_parquet(opt)
        assert restored[0]["price"] == 0.50
        assert restored[0]["size"] == 100.0

    def test_roundtrip_preserves_data(self):
        """Optimise → restore should yield the same data."""
        raw = [
            {
                "bids": [{"price": "0.48", "size": "30.0"}],
                "asks": [{"price": "0.52", "size": "25.0"}],
                "timestamp": "1765359900123",
            },
            {"price": "0.50", "size": "100.0", "side": "buy", "timestamp": "1765359901123"},
        ]
        opt = optimize_for_parquet(raw)
        restored = restore_from_parquet(opt)

        # Book entry
        assert restored[0]["bids"][0]["price"] == 0.48
        assert restored[0]["bids"][0]["size"] == 30.0
        assert restored[0]["timestamp"] == 1765359900123

        # Trade entry
        assert restored[1]["price"] == 0.50
        assert restored[1]["size"] == 100.0
        assert restored[1]["side"] == "buy"
        assert restored[1]["timestamp"] == 1765359901123

    def test_empty_data(self):
        assert optimize_for_parquet([]) == []
        assert restore_from_parquet([]) == []
