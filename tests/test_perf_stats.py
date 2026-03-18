"""Tests for the perf_stats module."""

from __future__ import annotations

import time
from keep.perf_stats import PerfStats


class TestPerfStats:

    def test_record_and_summary(self):
        ps = PerfStats(auto_log_interval_secs=99999)
        ps.record("action", "summarize", 0.1)
        ps.record("action", "summarize", 0.2)
        ps.record("action", "summarize", 0.3)

        s = ps.summary()
        assert "action:summarize" in s
        stats = s["action:summarize"]
        assert stats["count"] == 3
        assert abs(stats["total_s"] - 0.6) < 0.01
        assert abs(stats["mean_ms"] - 200.0) < 1.0

    def test_percentiles(self):
        ps = PerfStats(auto_log_interval_secs=99999)
        for i in range(100):
            ps.record("action", "find", i / 1000.0)  # 0ms to 99ms

        s = ps.summary()["action:find"]
        assert s["count"] == 100
        assert s["p50_ms"] >= 40  # ~49ms
        assert s["p50_ms"] <= 60
        assert s["p95_ms"] >= 85  # ~94ms

    def test_timer_context_manager(self):
        ps = PerfStats(auto_log_interval_secs=99999)
        with ps.timer("flow", "query-resolve"):
            time.sleep(0.01)

        s = ps.summary()
        assert "flow:query-resolve" in s
        assert s["flow:query-resolve"]["count"] == 1
        assert s["flow:query-resolve"]["total_s"] >= 0.005

    def test_separate_keys(self):
        ps = PerfStats(auto_log_interval_secs=99999)
        ps.record("action", "summarize", 0.1)
        ps.record("action", "analyze", 0.5)
        ps.record("flow", "after-write", 1.0)

        s = ps.summary()
        assert len(s) == 3
        assert s["action:summarize"]["count"] == 1
        assert s["action:analyze"]["count"] == 1
        assert s["flow:after-write"]["count"] == 1

    def test_reset(self):
        ps = PerfStats(auto_log_interval_secs=99999)
        ps.record("action", "x", 0.1)
        assert ps.summary()
        ps.reset()
        assert ps.summary() == {}

    def test_auto_log(self, caplog):
        import logging
        ps = PerfStats(auto_log_interval_secs=0)  # log on every record
        with caplog.at_level(logging.INFO, logger="keep.perf_stats"):
            ps.record("action", "a", 0.1)
            assert "Perf stats:" in caplog.text

    def test_summary_line_format(self):
        ps = PerfStats(auto_log_interval_secs=99999)
        ps.record("action", "tag", 0.05)
        ps.record("action", "tag", 0.15)

        s = ps.summary()["action:tag"]
        # Check all expected keys present
        assert set(s.keys()) == {"count", "total_s", "mean_ms", "p50_ms", "p95_ms", "max_ms"}
