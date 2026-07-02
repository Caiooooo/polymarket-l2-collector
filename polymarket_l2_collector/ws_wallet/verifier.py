"""RingBuffer real-time data verification engine."""
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Verdict:
    ts_sec: int
    max_bid_diff_pct: float
    max_ask_diff_pct: float
    action: str  # "ok" | "warn" | "switch"


@dataclass
class Bucket:
    ts_sec: int
    primary: list[dict] = field(default_factory=list)
    secondary: list[dict] = field(default_factory=list)

    @staticmethod
    def _top5_from(messages: list[dict], side: str) -> list[float]:
        """Extract up to 5 price values from the most recent messages."""
        prices: list[float] = []
        for msg in reversed(messages):
            for entry in msg.get(side, ()):
                if len(prices) >= 5:
                    break
                prices.append(float(entry["price"]))
            if len(prices) >= 5:
                break
        return prices

    def top5_prices(self, side: str) -> list[float]:
        """Return up to 5 price values from primary messages (most recent first)."""
        return self._top5_from(self.primary, side)

    @staticmethod
    def _max_diff(a: list[float], b: list[float]) -> float:
        """Max percentage difference between paired elements of a and b."""
        max_d = 0.0
        n = min(len(a), len(b))
        for i in range(n):
            denom = max(abs(a[i]), 0.0001)
            diff = abs(a[i] - b[i]) / denom * 100
            if diff > max_d:
                max_d = diff
        return max_d

    def compare(self, divergence_pct: float = 50.0) -> Verdict | None:
        """Compare primary and secondary data and return a Verdict, or None if insufficient data."""
        if not self.primary or not self.secondary:
            return None

        p_bids = self._top5_from(self.primary, "bids")
        s_bids = self._top5_from(self.secondary, "bids")
        p_asks = self._top5_from(self.primary, "asks")
        s_asks = self._top5_from(self.secondary, "asks")

        bid_diff = self._max_diff(p_bids, s_bids)
        ask_diff = self._max_diff(p_asks, s_asks)
        max_diff = max(bid_diff, ask_diff)

        if max_diff < 10:
            action = "ok"
        elif max_diff < divergence_pct:
            action = "warn"
        else:
            action = "switch"

        return Verdict(
            ts_sec=self.ts_sec,
            max_bid_diff_pct=bid_diff,
            max_ask_diff_pct=ask_diff,
            action=action,
        )


class Verifier:
    """Orchestrates primary/secondary WebSocket data verification."""

    def __init__(self, divergence_pct: float = 50.0):
        self._divergence_pct = divergence_pct
        self._buckets: dict[int, Bucket] = {}
        self._warn_count = 0

    def feed_primary(self, msg: dict) -> None:
        """Feed a primary WebSocket message into the ring buffer."""
        ts_sec = int(msg["timestamp"]) // 1000
        if ts_sec not in self._buckets:
            self._buckets[ts_sec] = Bucket(ts_sec=ts_sec)
        self._buckets[ts_sec].primary.append(msg)

    def feed_secondary(self, msg: dict) -> None:
        """Feed a secondary WebSocket message into the ring buffer."""
        ts_sec = int(msg["timestamp"]) // 1000
        if ts_sec not in self._buckets:
            self._buckets[ts_sec] = Bucket(ts_sec=ts_sec)
        self._buckets[ts_sec].secondary.append(msg)

    def tick(self) -> Verdict | None:
        """Check aged buckets and return the worst Verdict, or None if all are ok."""
        now_sec = int(time.time())
        ready = sorted(
            ts
            for ts, b in self._buckets.items()
            if ts < now_sec - 2 and b.primary and b.secondary
        )

        worst: Verdict | None = None
        for ts in ready:
            bucket = self._buckets.pop(ts)
            verdict = bucket.compare(self._divergence_pct)
            if verdict is None:
                continue
            if verdict.action == "warn":
                self._warn_count += 1
            if worst is None or self._verdict_rank(verdict) > self._verdict_rank(worst):
                worst = verdict

        if worst is not None and worst.action == "ok":
            return None
        return worst

    @staticmethod
    def _verdict_rank(v: Verdict) -> int:
        return {"ok": 0, "warn": 1, "switch": 2}.get(v.action, 0)

    @property
    def is_degraded(self) -> bool:
        """True when warn count exceeds the 300 threshold (~5 min at 1 tick/s)."""
        return self._warn_count > 300
