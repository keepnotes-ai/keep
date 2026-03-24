"""Daemon timer state persistence.

Tracks last-run timestamps for periodic daemon tasks so ``keep pending --list``
can show when each timer event last ran and when it's next scheduled.

The state file (``.timer_state.json``) lives alongside the store.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_FILENAME = ".timer_state.json"


def _state_path(store_path: Path) -> Path:
    return store_path / _FILENAME


def read_timer_state(store_path: Path) -> dict[str, dict[str, Any]]:
    """Read persisted timer state. Returns empty dict on any error."""
    p = _state_path(store_path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def write_timer_event(
    store_path: Path,
    name: str,
    *,
    interval: float,
    last_run: float | None = None,
    detail: str = "",
) -> None:
    """Record a timer event's last run time.

    Args:
        store_path: Store directory.
        name: Timer event name (e.g., "supernode-replenish", "watch-poll").
        interval: Interval in seconds between runs.
        last_run: Unix timestamp of last run. Defaults to now.
        detail: Optional detail string (e.g., "5 enqueued").
    """
    p = _state_path(store_path)
    try:
        state = json.loads(p.read_text()) if p.exists() else {}
    except Exception:
        state = {}

    state[name] = {
        "last_run": last_run or time.time(),
        "interval": interval,
        "detail": detail,
    }

    try:
        p.write_text(json.dumps(state, indent=2) + "\n")
    except Exception as exc:
        logger.debug("Failed to write timer state: %s", exc)


def _format_ago(seconds: float) -> str:
    """Format a duration as a human-readable 'ago' string."""
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds / 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds / 3600)}h ago"
    return f"{int(seconds / 86400)}d ago"


def _format_until(seconds: float) -> str:
    """Format a duration as a human-readable 'in X' string."""
    if seconds <= 0:
        return "due now"
    if seconds < 60:
        return f"in {int(seconds)}s"
    if seconds < 3600:
        return f"in {int(seconds / 60)}m"
    return f"in {int(seconds / 3600)}h"


# Well-known daemon timer events with their default intervals.
# These show up in pending --list even before first fire.
KNOWN_TIMERS: dict[str, dict[str, Any]] = {
    "supernode-replenish": {
        "interval": 1800,
        "description": "find and enqueue supernode review candidates",
    },
}


def format_timer_events(store_path: Path, watches: list | None = None) -> list[str]:
    """Format timer events for display.

    Shows both persisted timer events (from daemon runs) and known
    timers that haven't fired yet (scheduled but never run).

    Returns a list of formatted lines, one per timer event.
    """
    state = read_timer_state(store_path)
    lines: list[str] = []
    now = time.time()

    # Start with persisted timer events
    shown: set[str] = set()
    events = sorted(state.items(), key=lambda kv: kv[1].get("last_run", 0), reverse=True)
    for name, info in events:
        shown.add(name)
        last_run = info.get("last_run", 0)
        interval = info.get("interval", 0)
        detail = info.get("detail", "")

        ago_str = _format_ago(now - last_run) if last_run else "never"

        next_str = ""
        if interval and last_run:
            next_str = _format_until(last_run + interval - now)

        parts = [f"  {name:24s} last: {ago_str:>8s}"]
        if next_str:
            parts.append(f"  next: {next_str}")
        if detail:
            parts.append(f"  ({detail})")
        lines.append("".join(parts))

    # Show known timers that haven't fired yet
    for name, info in KNOWN_TIMERS.items():
        if name in shown:
            continue
        desc = info.get("description", "")
        interval = info.get("interval", 0)
        interval_str = _format_until(interval) if interval else ""
        lines.append(f"  {name:24s} last:    never  next: due now  {desc}")

    return lines
