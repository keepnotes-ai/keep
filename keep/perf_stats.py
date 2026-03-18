"""Lightweight runtime performance statistics.

Tracks wall-clock timing per action and flow, computes basic aggregates
(count, total, mean, p50, p95, max), and logs periodic summaries.

Usage::

    from keep.perf_stats import perf

    with perf.timer("action", "summarize"):
        result = action.run(params, ctx)

    perf.log_summary()  # explicit; also auto-logs every N records
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Any, Generator

logger = logging.getLogger(__name__)

# Auto-log interval in seconds.  Logs at most once per interval
# regardless of how many samples are recorded (avoids spam from
# fast tasks while still reporting during slow LLM work).
_AUTO_LOG_INTERVAL_SECS = 300  # 5 minutes

# Maximum samples retained per key (for percentile computation).
_MAX_SAMPLES = 500


class _TimingSeries:
    """Collects wall-clock durations for a single key."""

    __slots__ = ("count", "total", "min", "max", "max_id", "_samples")

    def __init__(self) -> None:
        self.count: int = 0
        self.total: float = 0.0
        self.min: float = float("inf")
        self.max: float = 0.0
        self.max_id: str | None = None
        self._samples: deque[float] = deque(maxlen=_MAX_SAMPLES)

    def record(self, duration: float, context_id: str | None = None) -> None:
        self.count += 1
        self.total += duration
        if duration < self.min:
            self.min = duration
        if duration > self.max:
            self.max = duration
            self.max_id = context_id
        self._samples.append(duration)

    def percentile(self, p: float) -> float:
        if not self._samples:
            return 0.0
        s = sorted(self._samples)
        idx = min(int(len(s) * p / 100), len(s) - 1)
        return s[idx]

    def mean(self) -> float:
        return self.total / self.count if self.count else 0.0

    def summary_line(self) -> str:
        if not self.count:
            return "n=0"
        max_suffix = f" ({self.max_id})" if self.max_id else ""
        return (
            f"n={self.count} "
            f"total={self.total:.2f}s "
            f"mean={self.mean() * 1000:.0f}ms "
            f"p50={self.percentile(50) * 1000:.0f}ms "
            f"p95={self.percentile(95) * 1000:.0f}ms "
            f"max={self.max * 1000:.0f}ms{max_suffix}"
        )

    def summary_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "total_s": round(self.total, 3),
            "mean_ms": round(self.mean() * 1000, 1),
            "p50_ms": round(self.percentile(50) * 1000, 1),
            "p95_ms": round(self.percentile(95) * 1000, 1),
            "max_ms": round(self.max * 1000, 1) if self.count else 0.0,
        }


class PerfStats:
    """Process-wide performance statistics tracker."""

    def __init__(self, auto_log_interval_secs: float = _AUTO_LOG_INTERVAL_SECS) -> None:
        self._lock = threading.Lock()
        self._series: dict[str, _TimingSeries] = {}
        self._auto_log_interval = auto_log_interval_secs
        self._last_auto_log: float = 0.0

    def record(self, category: str, key: str, duration: float, context_id: str | None = None) -> None:
        """Record a timing sample, optionally tagging it with a context ID."""
        label = f"{category}:{key}"
        now = time.monotonic()
        with self._lock:
            ts = self._series.get(label)
            if ts is None:
                ts = _TimingSeries()
                self._series[label] = ts
            ts.record(duration, context_id)
            if now - self._last_auto_log >= self._auto_log_interval:
                self._log_unlocked()
                self._last_auto_log = now

    @contextmanager
    def timer(self, category: str, key: str, context_id: str | None = None) -> Generator[None, None, None]:
        """Context manager that records elapsed wall-clock time."""
        t0 = time.monotonic()
        try:
            yield
        finally:
            self.record(category, key, time.monotonic() - t0, context_id)

    def log_summary(self) -> None:
        """Log current stats summary."""
        with self._lock:
            self._log_unlocked()

    def _log_unlocked(self) -> None:
        if not self._series:
            return
        lines = ["Perf stats:"]
        for label in sorted(self._series):
            lines.append(f"  {label}: {self._series[label].summary_line()}")
        logger.info("\n".join(lines))

    def summary(self) -> dict[str, dict[str, Any]]:
        """Return all series as a dict (for programmatic access)."""
        with self._lock:
            return {
                label: ts.summary_dict()
                for label, ts in sorted(self._series.items())
            }

    def reset(self) -> None:
        """Clear all recorded data."""
        with self._lock:
            self._series.clear()
            self._ops_since_log = 0


# Module-level singleton.
perf = PerfStats()
