"""Shared HTTP session for connection pooling across all providers."""

import requests

_session: requests.Session | None = None


def http_session() -> requests.Session:
    """Return a shared requests.Session for all provider HTTP calls.

    Reuses TCP connections across embedding, summarization, tagging,
    document fetch, and other HTTP-backed provider calls.
    """
    global _session
    if _session is None:
        from keep.types import user_agent

        _session = requests.Session()
        _session.headers["User-Agent"] = user_agent()
    return _session


def close_http_session() -> None:
    """Close the shared session, interrupting any in-flight requests.

    Called during shutdown to unblock threads stuck in socket reads.
    """
    global _session
    if _session is not None:
        try:
            _session.close()
        except Exception:
            pass
        _session = None
