"""Process-global shutdown coordination.

Any thread can check ``is_shutting_down()`` to cooperate with graceful
shutdown.  The daemon signal handler calls ``request_shutdown()`` which
sets the event, waking any thread blocked on ``wait()``.

Usage in long-running code paths::

    from keep.shutdown import is_shutting_down
    for item in work:
        if is_shutting_down():
            break
        process(item)

Usage in sleeps::

    from keep.shutdown import wait_or_shutdown
    wait_or_shutdown(60.0)  # returns True if shutdown, False if timeout
"""

import threading

_event = threading.Event()


def request_shutdown() -> None:
    """Signal all threads to stop."""
    _event.set()


def is_shutting_down() -> bool:
    """Check if shutdown has been requested."""
    return _event.is_set()


def wait_or_shutdown(timeout: float) -> bool:
    """Sleep for *timeout* seconds, waking immediately on shutdown.

    Returns True if woken by shutdown, False if timeout elapsed.
    """
    return _event.wait(timeout=timeout)
