"""Tests for Verifier, Bucket, and Verdict."""

from __future__ import annotations

import time

from polymarket_l2_collector.ws_wallet.verifier import Bucket, Verifier


def _book_msg(bids=None, asks=None, ts_ms=None):
    return {
        "asset_id": "123",
        "bids": bids or [{"price": "100.0", "size": "1.0"}],
        "asks": asks or [{"price": "101.0", "size": "1.0"}],
        "timestamp": str(ts_ms or int(time.time() * 1000)),
    }


class TestBucket:
    """Tests for the Bucket data structure."""

    def test_top5_bids_returns_prices(self):
        """3 bids -> 3 prices"""
        bucket = Bucket(ts_sec=100, primary=[], secondary=[])
        bids = [
            {"price": "1.0", "size": "1.0"},
            {"price": "2.0", "size": "1.0"},
            {"price": "3.0", "size": "1.0"},
        ]
        bucket.primary.append({"bids": bids, "asks": [], "timestamp": "100000"})
        prices = bucket.top5_prices("bids")
        assert prices == [1.0, 2.0, 3.0]

    def test_top5_bids_max_five(self):
        """10 bids -> 5 prices"""
        bucket = Bucket(ts_sec=100, primary=[], secondary=[])
        bids = [{"price": str(float(i)), "size": "1.0"} for i in range(10)]
        bucket.primary.append({"bids": bids, "asks": [], "timestamp": "100000"})
        prices = bucket.top5_prices("bids")
        assert len(prices) == 5
        assert prices == [0.0, 1.0, 2.0, 3.0, 4.0]

    def test_top5_asks_ascending(self):
        """3 asks -> 3 prices"""
        bucket = Bucket(ts_sec=100, primary=[], secondary=[])
        asks = [
            {"price": "100.0", "size": "1.0"},
            {"price": "101.0", "size": "1.0"},
            {"price": "102.0", "size": "1.0"},
        ]
        bucket.primary.append({"bids": [], "asks": asks, "timestamp": "100000"})
        prices = bucket.top5_prices("asks")
        assert prices == [100.0, 101.0, 102.0]

    def test_compare_identical_returns_ok_verdict(self):
        """identical primary/secondary -> Verdict with action='ok'"""
        msg = _book_msg(
            bids=[{"price": "100.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
        )
        bucket = Bucket(ts_sec=1000, primary=[msg], secondary=[msg])
        result = bucket.compare()
        assert result is not None
        assert result.action == "ok"
        assert result.ts_sec == 1000

    def test_compare_large_divergence_returns_switch(self):
        """divergence >= 50% -> switch"""
        msg1 = _book_msg(
            bids=[{"price": "100.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
        )
        msg2 = _book_msg(
            bids=[{"price": "30.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
        )
        bucket = Bucket(ts_sec=1000, primary=[msg1], secondary=[msg2])
        result = bucket.compare()
        assert result is not None
        assert result.action == "switch"

    def test_compare_moderate_divergence_returns_warn(self):
        """10% <= divergence < 50% -> warn"""
        msg1 = _book_msg(
            bids=[{"price": "100.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
        )
        msg2 = _book_msg(
            bids=[{"price": "80.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
        )
        bucket = Bucket(ts_sec=1000, primary=[msg1], secondary=[msg2])
        result = bucket.compare()
        assert result is not None
        assert result.action == "warn"

    def test_empty_bucket_returns_none(self):
        """no primary data -> None"""
        bucket = Bucket(ts_sec=1000, primary=[], secondary=[])
        result = bucket.compare()
        assert result is None

    def test_no_secondary_returns_none(self):
        """only primary data -> None"""
        msg = _book_msg()
        bucket = Bucket(ts_sec=1000, primary=[msg], secondary=[])
        result = bucket.compare()
        assert result is None


class TestVerifier:
    """Tests for the Verifier orchestrator."""

    def test_feed_and_tick_ok(self):
        """identical primary/secondary -> None (no anomaly)"""
        v = Verifier()
        ts = int(time.time() * 1000) - 5000
        msg = _book_msg(ts_ms=ts)
        v.feed_primary(msg)
        v.feed_secondary(msg)
        result = v.tick()
        assert result is None

    def test_divergent_primary_secondary_triggers_warn(self):
        """divergent data -> warn verdict"""
        v = Verifier()
        ts = int(time.time() * 1000) - 5000
        msg1 = _book_msg(
            bids=[{"price": "100.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
            ts_ms=ts,
        )
        msg2 = _book_msg(
            bids=[{"price": "80.0", "size": "1.0"}],
            asks=[{"price": "101.0", "size": "1.0"}],
            ts_ms=ts,
        )
        v.feed_primary(msg1)
        v.feed_secondary(msg2)
        result = v.tick()
        assert result is not None
        assert result.action in ("warn", "switch")

    def test_is_degraded_after_many_warns(self):
        """_warn_count > 300 -> is_degraded == True"""
        v = Verifier()
        v._warn_count = 301
        assert v.is_degraded is True
        v._warn_count = 300
        assert v.is_degraded is False
