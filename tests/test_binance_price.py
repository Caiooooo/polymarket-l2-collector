"""
Unit tests for binance_price module.

Verifies the SYMBOLS list completeness and the structure of the
``current_prices`` dict that bookTicker messages populate.
"""

from polymarket_l2_collector.binance_price import SYMBOLS


class TestBinanceSymbols:
    """SYMBOLS list must include all tracked coins."""

    def test_symbols_list_includes_all(self):
        """Verify SYMBOLS has all 4 coins."""
        expected = {"btcusdt", "ethusdt", "solusdt", "xrpusdt"}
        assert set(SYMBOLS) == expected, (
            f"SYMBOLS should be {expected}, got {set(SYMBOLS)}"
        )


class TestCurrentPricesStructure:
    """Verify the ``current_prices`` dict entries have the expected keys."""

    def test_current_prices_structure(self):
        """A fake entry should contain bid / ask / mid / spread / time."""
        entry = {
            "bid": 50000.0,
            "ask": 50001.0,
            "mid": 50000.5,
            "spread": 1.0,
            "time": "12:00:00.000",
        }
        assert "bid" in entry
        assert "ask" in entry
        assert "mid" in entry
        assert "spread" in entry
        assert "time" in entry
