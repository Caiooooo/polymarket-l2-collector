"""
Unit tests for WS client utilities.
"""

from polymarket_l2_collector.ws_client import build_asset_id_list


class TestBuildAssetIdList:
    """Asset ID list extraction from asset dict."""

    def test_single_coin_single_interval_up(self):
        assets = {
            "btc": {
                "5m": {"up": "id_btc_5m_up", "down": "id_btc_5m_down"},
            }
        }
        ids = build_asset_id_list(assets, ["up"])
        assert ids == ["id_btc_5m_up"]

    def test_both_directions(self):
        assets = {
            "btc": {
                "5m": {"up": "id_up", "down": "id_down"},
            }
        }
        ids = build_asset_id_list(assets, ["up", "down"])
        assert "id_up" in ids
        assert "id_down" in ids

    def test_multiple_coins_and_intervals(self):
        assets = {
            "btc": {
                "5m": {"up": "id_btc_5m_up"},
                "15m": {"up": "id_btc_15m_up"},
            },
            "eth": {
                "5m": {"up": "id_eth_5m_up"},
            },
        }
        ids = build_asset_id_list(assets, ["up"])
        assert len(ids) == 3
        assert "id_btc_5m_up" in ids
        assert "id_btc_15m_up" in ids
        assert "id_eth_5m_up" in ids

    def test_empty_assets(self):
        assert build_asset_id_list({}, ["up"]) == []

    def test_missing_direction(self):
        assets = {"btc": {"5m": {"up": "id_up"}}}
        ids = build_asset_id_list(assets, ["down"])
        assert ids == []

    def test_malformed_interval_data(self):
        """If interval data is not a dict, skip it."""
        assets = {"btc": {"5m": None}}
        ids = build_asset_id_list(assets, ["up"])
        assert ids == []
