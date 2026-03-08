"""Error logging utilities for keep CLI.

Logs full stack traces for debugging while showing clean messages to users.
"""

import os
import traceback
from datetime import datetime, timezone
from pathlib import Path


def _error_log_path() -> Path:
    """Resolve error log path, respecting KEEP_STORE_PATH."""
    store = os.environ.get("KEEP_STORE_PATH")
    if store:
        return Path(store) / "keep-errors.log"
    return Path.home() / ".keep" / "keep-errors.log"


def log_exception(exc: Exception, context: str = "") -> Path:
    """Log exception with full traceback to file.

    Args:
        exc: The exception that occurred
        context: Optional context string (e.g., command name)

    Returns:
        Path to the error log file
    """
    log_path = _error_log_path()
    timestamp = datetime.now(timezone.utc).isoformat()
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"[{timestamp}]")
            if context:
                f.write(f" {context}")
            f.write("\n")
            f.write(traceback.format_exc())
    except OSError:
        pass  # Can't write error log — don't crash over it
    return log_path
