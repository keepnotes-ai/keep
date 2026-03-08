"""Cross-process model lock using fcntl.flock.

Prevents multiple keep processes from loading MLX models simultaneously,
which would exhaust GPU memory on Apple Silicon.

Uses POSIX advisory file locks which are automatically released on
process exit (including crashes), so stale locks cannot occur.
"""

import gc
import logging
import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# fcntl is only available on POSIX systems
_has_fcntl = sys.platform != "win32"
if _has_fcntl:
    import fcntl


class ModelLock:
    """Advisory file lock for serializing model access across processes.

    Uses fcntl.flock which is:
    - Automatically released on process exit/crash
    - Blocking by default (waits until lock available)
    - Safe on macOS and Linux

    On non-POSIX platforms, acquire/release are no-ops.
    """

    def __init__(self, lock_path: Path):
        self._lock_path = lock_path
        self._fd: Optional[int] = None

    def acquire(self, blocking: bool = True, timeout: float = 30) -> bool:
        """Acquire the lock.

        Args:
            blocking: If True, wait until lock is available (up to timeout).
                      If False, return False immediately if lock is held.
            timeout: Maximum seconds to wait when blocking (default 30).
                     Prevents deadlock when another process is stuck holding
                     the lock.

        Returns:
            True if lock was acquired, False if non-blocking and lock is held.

        Raises:
            TimeoutError: If blocking and lock not acquired within timeout.
        """
        if not _has_fcntl:
            return True

        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._lock_path), os.O_CREAT | os.O_RDWR)

        if not blocking:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug("Acquired model lock: %s", self._lock_path.name)
                return True
            except (OSError, BlockingIOError):
                os.close(self._fd)
                self._fd = None
                return False

        # Blocking with timeout: poll with non-blocking flock
        deadline = time.monotonic() + timeout
        interval = 0.1
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                logger.debug("Acquired model lock: %s", self._lock_path.name)
                return True
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    os.close(self._fd)
                    self._fd = None
                    raise TimeoutError(
                        f"Timed out waiting for model lock ({self._lock_path.name}). "
                        f"Another keep process may be stuck — check with: "
                        f"lsof {self._lock_path}"
                    )
                time.sleep(interval)
                # Back off: 0.1, 0.2, 0.4, 0.5, 0.5, ...
                interval = min(interval * 2, 0.5)

    def release(self) -> None:
        """Release the lock."""
        if not _has_fcntl or self._fd is None:
            return

        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
        except OSError:
            pass
        self._fd = None
        logger.debug("Released model lock: %s", self._lock_path.name)

    def is_locked(self) -> bool:
        """Probe whether the lock is currently held by another process.

        Does not acquire the lock. Returns True if held, False if available.
        """
        if not _has_fcntl:
            return False

        if not self._lock_path.exists():
            return False

        fd = None
        try:
            fd = os.open(str(self._lock_path), os.O_RDWR)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Got the lock — nobody holds it. Release immediately.
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        except (OSError, BlockingIOError):
            # Lock is held by another process.
            return True
        finally:
            if fd is not None:
                os.close(fd)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False


class LockedEmbeddingProvider:
    """Per-call locked wrapper for an embedding provider.

    Acquires a file lock only during embed() calls to serialize GPU
    access across processes. The lock is released after each call so
    concurrent processes only block briefly during actual computation,
    not for the entire process lifetime.

    The CachingEmbeddingProvider should wrap OUTSIDE this class so
    that cache hits never touch the lock or the model.
    """

    def __init__(self, provider, lock_path: Path):
        self._provider = provider
        self._lock = ModelLock(lock_path)
        self._thread_lock = threading.RLock()

    @property
    def model_name(self) -> str:
        return getattr(self._provider, "model_name", "unknown")

    @property
    def dimension(self) -> int:
        with self._thread_lock:
            with self._lock:
                return self._provider.dimension

    def embed(self, text: str) -> list[float]:
        with self._thread_lock:
            with self._lock:
                return self._provider.embed(text)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        with self._thread_lock:
            with self._lock:
                return self._provider.embed_batch(texts)

    def release(self) -> None:
        """Free the model."""
        with self._thread_lock:
            try:
                del self._provider
            except AttributeError:
                pass
            self._provider = None
            gc.collect()
            logger.debug("Released embedding provider")


class LockedSummarizationProvider:
    """Per-call locked wrapper for a summarization provider.

    Same pattern as LockedEmbeddingProvider — acquires the lock only
    during summarize() calls.
    """

    def __init__(self, provider, lock_path: Path):
        self._provider = provider
        self._lock = ModelLock(lock_path)
        self._thread_lock = threading.RLock()

    @property
    def model_name(self) -> str:
        return getattr(self._provider, "model_name", "unknown")

    def summarize(
        self,
        content: str,
        *,
        max_length: int = 500,
        context: str | None = None,
    ) -> str:
        with self._thread_lock:
            with self._lock:
                return self._provider.summarize(
                    content, max_length=max_length, context=context
                )

    def release(self) -> None:
        """Free the model."""
        with self._thread_lock:
            try:
                del self._provider
            except AttributeError:
                pass
            self._provider = None
            gc.collect()
            logger.debug("Released summarization provider")


class LockedMediaDescriber:
    """Per-call locked wrapper for a media describer.

    Same pattern as LockedSummarizationProvider — acquires the lock only
    during describe() calls.
    """

    def __init__(self, provider, lock_path: Path):
        self._provider = provider
        self._lock = ModelLock(lock_path)
        self._thread_lock = threading.RLock()

    @property
    def model_name(self) -> str:
        return getattr(self._provider, "model_name", "unknown")

    def describe(self, path: str, content_type: str) -> str | None:
        with self._thread_lock:
            with self._lock:
                return self._provider.describe(path, content_type)

    def release(self) -> None:
        """Free the model."""
        with self._thread_lock:
            try:
                del self._provider
            except AttributeError:
                pass
            self._provider = None
            gc.collect()
            logger.debug("Released media describer")


class LockedContentExtractor:
    """Per-call locked wrapper for a content extractor.

    Same pattern as LockedMediaDescriber — acquires the lock only
    during extract() calls.
    """

    def __init__(self, provider, lock_path: Path):
        self._provider = provider
        self._lock = ModelLock(lock_path)
        self._thread_lock = threading.RLock()

    @property
    def model_name(self) -> str:
        return getattr(self._provider, "model_name", "unknown")

    def extract(self, path: str, content_type: str) -> str | None:
        with self._thread_lock:
            with self._lock:
                return self._provider.extract(path, content_type)

    def release(self) -> None:
        """Free the model."""
        with self._thread_lock:
            try:
                del self._provider
            except AttributeError:
                pass
            self._provider = None
            gc.collect()
            logger.debug("Released content extractor")
