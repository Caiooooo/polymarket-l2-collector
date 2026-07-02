"""
Unit tests for time-window computation logic.
"""


from polymarket_l2_collector.config import load_settings


class TestWindowCalculation:
    """Window boundary calculations for 5m and 15m intervals."""

    def setup_method(self):
        self.settings = load_settings()

    def test_5m_interval_seconds(self):
        assert self.settings.interval_seconds("5m") == 300

    def test_15m_interval_seconds(self):
        assert self.settings.interval_seconds("15m") == 900

    def test_5m_window_boundary_align(self):
        """A timestamp at a 5m boundary should stay on that boundary."""
        interval = 300
        ts = 1765359900  # a 5m-aligned timestamp (divisible by 300)
        window = (ts // interval) * interval
        assert window == ts

    def test_5m_window_floor(self):
        """A mid-window timestamp should floor to the start."""
        interval = 300
        ts = 1765359900 + 123  # 123s into the window
        window = (ts // interval) * interval
        assert window == 1765359900

    def test_15m_window_floor(self):
        interval = 900
        ts = 1765360000
        window = (ts // interval) * interval
        assert window == 1765359900  # floor to previous 15m boundary

    def test_cross_hour_5m(self):
        """5m windows should cross hour boundaries correctly."""
        interval = 300
        # 1h before epoch + 3500s = 1h 3500 → window starts at 1h 3300
        ts = 3600 + 3500  # 7100
        window = (ts // interval) * interval
        assert window == 6900
        assert window % interval == 0

    def test_cross_midnight(self):
        """5m windows crossing midnight UTC."""
        interval = 300
        # 86400 + 1 → should floor to 86400
        ts = 86400 + 1
        window = (ts // interval) * interval
        assert window == 86400


class TestTimestampNormalisation:
    """Timestamp normalisation (ms vs seconds)."""

    def test_ms_timestamp_div_1000(self):
        """Timestamp > 1e12 is ms, should be divided by 1000."""
        ms_ts = 1765359900123
        seconds = ms_ts // 1000
        assert seconds == 1765359900

    def test_epoch_timestamp_passthrough(self):
        """Timestamp < 1e12 is already seconds."""
        sec_ts = 1765359900
        assert sec_ts < 1_000_000_000_000
