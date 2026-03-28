"""Shared daemon HTTP client — used by thin_cli and mcp.

Stdlib-only (no typer, no keep internals). Provides daemon discovery,
auto-start, health check, and HTTP request with retry.
"""

import http.client
import json
import os
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_PORT = 5337


_auth_token: str = ""


def _load_token(store_override: str | None = None, *, force: bool = False) -> str:
    """Read the daemon auth token from .daemon.token."""
    global _auth_token
    if _auth_token and not force:
        return _auth_token
    _auth_token = ""
    store = resolve_store_path(store_override)
    token_file = store / ".daemon.token"
    if token_file.exists():
        try:
            _auth_token = token_file.read_text().strip()
        except OSError:
            pass
    return _auth_token


def http_request(
    method: str, port: int, path: str,
    body: dict | None = None, timeout: int = 30,
) -> tuple[int, dict]:
    """Make an HTTP request to the daemon. Returns (status, json_body).

    Retries once on transient connection errors (daemon may be busy with
    initial setup when the first request arrives).
    """
    headers: dict[str, str] = {}
    if _auth_token:
        headers["Authorization"] = f"Bearer {_auth_token}"
    # Propagate trace context to daemon (W3C traceparent)
    try:
        from opentelemetry.propagate import inject
        inject(headers)
    except Exception:
        pass
    data = None
    if body is not None:
        data = json.dumps({k: v for k, v in body.items() if v is not None})
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(data))

    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
            conn.request(method, path, data, headers)
            resp = conn.getresponse()
            result = json.loads(resp.read())
            status = resp.status
            conn.close()
            if status == 401 and attempt == 0:
                # Token may be stale (daemon restarted). Re-read and retry.
                _load_token(force=True)
                if _auth_token:
                    headers["Authorization"] = f"Bearer {_auth_token}"
                continue
            return status, result
        except (ConnectionError, TimeoutError, http.client.RemoteDisconnected, OSError) as exc:
            last_exc = exc
            try:
                conn.close()
            except Exception:
                pass
            if attempt == 0:
                time.sleep(0.2)
    raise last_exc  # type: ignore[misc]


def resolve_store_path(override: str | None = None) -> Path:
    """Resolve store path from override, env, or config. Stdlib only."""
    effective = override or os.environ.get("KEEP_STORE_PATH")
    if effective:
        return Path(effective).resolve()
    config_dir = (
        Path(os.environ["KEEP_CONFIG"])
        if os.environ.get("KEEP_CONFIG")
        else Path.home() / ".keep"
    )
    config_file = config_dir / "keep.toml"
    if config_file.exists():
        try:
            import tomllib
            with open(config_file, "rb") as f:
                data = tomllib.load(f)
            val = data.get("store", {}).get("path")
            if val:
                return Path(val).expanduser().resolve()
        except Exception:
            pass
    return config_dir.resolve()


_warnings_shown: bool = False


def check_health(port: int) -> bool:
    """Check daemon health + setup in a single round-trip.

    Returns True if healthy. Prints warnings to stderr (once).
    Calls sys.exit(1) if setup is needed.
    """
    global _warnings_shown
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        headers = {}
        if _auth_token:
            headers["Authorization"] = f"Bearer {_auth_token}"
        conn.request("GET", "/v1/health", headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        if resp.status == 401:
            return False  # stale token — daemon restarted
        if resp.status != 200:
            return False
        health = json.loads(raw)
        if health.get("needs_setup"):
            print("keep is not configured. Run: keep config --setup", file=sys.stderr)
            sys.exit(1)
        if not _warnings_shown:
            for warning in health.get("warnings", []):
                print(f"Warning: {warning}", file=sys.stderr)
            _warnings_shown = True
        return True
    except SystemExit:
        raise
    except Exception:
        return False


def start_daemon(store_path: Path) -> None:
    """Spawn daemon process."""
    cmd = [sys.executable, "-m", "keep.daemon", "--store", str(store_path)]
    log_path = store_path / "keep-ops.log"
    store_path.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a") as log_fd:
        kwargs: dict = {"stdout": subprocess.DEVNULL, "stderr": log_fd, "stdin": subprocess.DEVNULL}
        if sys.platform != "win32":
            kwargs["start_new_session"] = True
        else:
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        subprocess.Popen(cmd, **kwargs)


def get_port(store_override: str | None = None) -> int:
    """Get daemon port, auto-starting if needed. Loads auth token."""
    store_path = resolve_store_path(store_override)
    port_file = store_path / ".daemon.port"

    # Load auth token for subsequent HTTP requests
    _load_token(store_override)

    # Try existing daemon
    if port_file.exists():
        try:
            port = int(port_file.read_text().strip())
            if check_health(port):
                return port
        except (ValueError, OSError):
            pass

    # Auto-start daemon (generates new token)
    global _auth_token
    _auth_token = ""  # clear stale token
    # Remove stale token/port files so we don't poll with old credentials
    (store_path / ".daemon.token").unlink(missing_ok=True)
    port_file.unlink(missing_ok=True)
    print("Starting daemon...", file=sys.stderr)
    start_daemon(store_path)

    # Poll for readiness
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        # Re-read token each iteration — daemon writes it at startup
        _auth_token = ""
        _load_token(store_override)
        if not _auth_token:
            time.sleep(0.1)
            continue
        if port_file.exists():
            try:
                port = int(port_file.read_text().strip())
                if check_health(port):
                    return port
            except (ValueError, OSError):
                pass
        time.sleep(0.1)

    print("Error: daemon did not start in time.", file=sys.stderr)
    sys.exit(1)
