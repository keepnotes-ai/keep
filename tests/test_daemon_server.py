"""Tests for the daemon HTTP query server.

Uses mock_providers to avoid loading real ML models.
Tests both raw HTTP and RemoteKeeper round-trip.
"""

import json
import socket

import httpx
import pytest

from keep.api import Keeper
from keep.daemon_server import DaemonServer


@pytest.fixture
def daemon(mock_providers, tmp_path):
    """Start a DaemonServer on an OS-assigned port."""
    kp = Keeper(store_path=tmp_path)
    server = DaemonServer(kp, port=0)
    port = server.start()
    yield server, kp, port
    server.stop()
    kp.close()


@pytest.fixture
def http(daemon):
    """httpx.Client with base_url and auth token pre-configured."""
    server, _, port = daemon
    client = httpx.Client(
        base_url=f"http://127.0.0.1:{port}",
        headers={"Authorization": f"Bearer {server.auth_token}"},
        timeout=5,
    )
    yield client
    client.close()


# --- Health ---

def test_health(http):
    r = http.get("/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "pid" in body
    assert "version" in body
    assert "store" in body
    assert "embedding" in body
    assert "needs_setup" in body
    assert "warnings" in body
    assert isinstance(body["warnings"], list)


def test_401_without_token(daemon):
    _, _, port = daemon
    r = httpx.get(f"http://127.0.0.1:{port}/v1/health", timeout=5)
    assert r.status_code == 401


def test_404_unknown_path(http):
    r = http.get("/v1/nonexistent")
    assert r.status_code == 404


# --- Put / Get / Delete ---

def test_put_and_get(http):
    r = http.post("/v1/notes", json={
        "content": "test note", "id": "test-1", "tags": {"topic": "cache"},
    })
    assert r.status_code == 200
    assert r.json()["id"] == "test-1"

    r = http.get("/v1/notes/test-1")
    assert r.status_code == 200
    assert r.json()["id"] == "test-1"

    r = http.get("/v1/notes/nonexistent")
    assert r.status_code == 404


def test_delete(http):
    http.post("/v1/notes", json={"content": "to delete", "id": "del-1"})
    r = http.delete("/v1/notes/del-1")
    assert r.status_code == 200
    assert r.json()["deleted"] is True

    r = http.get("/v1/notes/del-1")
    assert r.status_code == 404


# --- Tag ---

def test_tag(http):
    http.post("/v1/notes", json={"content": "tag test", "id": "tag-1"})
    r = http.patch("/v1/notes/tag-1/tags", json={"set": {"color": "blue"}})
    assert r.status_code == 200
    assert r.json()["tags"].get("color") == "blue"


# --- Find ---

def test_find(http):
    http.post("/v1/notes", json={"content": "alpha beta", "id": "s-1"})
    r = http.post("/v1/search", json={"query": "alpha", "limit": 5})
    assert r.status_code == 200
    assert "notes" in r.json()


# --- Flow ---

def test_flow(http):
    http.post("/v1/notes", json={"content": "flow test", "id": "f-1"})
    r = http.post("/v1/flow", json={
        "state": "get",
        "params": {"item_id": "f-1", "similar_limit": 1, "meta_limit": 1,
                   "parts_limit": 0, "edges_limit": 0, "versions_limit": 0},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "done"
    assert "bindings" in body


# --- Port fallback ---

def test_port_fallback(mock_providers, tmp_path):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    occupied_port = sock.getsockname()[1]
    sock.listen(1)
    try:
        kp = Keeper(store_path=tmp_path)
        server = DaemonServer(kp, port=occupied_port)
        actual_port = server.start()
        assert actual_port != occupied_port
        r = httpx.get(
            f"http://127.0.0.1:{actual_port}/v1/health",
            headers={"Authorization": f"Bearer {server.auth_token}"},
            timeout=5,
        )
        assert r.status_code == 200
        server.stop()
        kp.close()
    finally:
        sock.close()


# --- RemoteKeeper round-trip ---

def test_remote_keeper_round_trip(daemon):
    server, kp, port = daemon
    from keep.remote import RemoteKeeper

    client = RemoteKeeper(
        api_url=f"http://127.0.0.1:{port}",
        api_key=server.auth_token, config=kp.config)

    item = client.put(content="round trip test", id="rt-1", tags={"status": "test"})
    assert item.id == "rt-1"

    item = client.get("rt-1")
    assert item is not None
    assert item.id == "rt-1"

    results = client.find(query="round trip")
    assert isinstance(results, list)

    tagged = client.tag("rt-1", {"color": "red"})
    assert tagged is not None

    assert client.exists("rt-1")
    assert not client.exists("nonexistent")
    assert client.delete("rt-1") is True

    client.close()


def test_context_endpoint(http):
    """The /context endpoint returns full ItemContext in one call."""
    http.post("/v1/notes", json={"content": "context endpoint test", "id": "ce-1"})
    r = http.get("/v1/notes/ce-1/context", params={"similar_limit": 2, "edges_limit": 1})
    assert r.status_code == 200
    body = r.json()
    assert body["item"]["id"] == "ce-1"
    assert "similar" in body
    assert "meta" in body
    assert "parts" in body
    assert "prev" in body

    r = http.get("/v1/notes/nonexistent/context")
    assert r.status_code == 404


def test_remote_keeper_get_context_via_flow(daemon):
    """get_context() uses get + flow endpoint."""
    server, kp, port = daemon
    from keep.remote import RemoteKeeper

    client = RemoteKeeper(
        api_url=f"http://127.0.0.1:{port}",
        api_key=server.auth_token, config=kp.config)

    client.put(content="context test note", id="ctx-1")
    ctx = client.get_context("ctx-1", edges_limit=2, parts_limit=5)
    assert ctx is not None
    assert ctx.item.id == "ctx-1"
    assert isinstance(ctx.similar, list)
    assert isinstance(ctx.meta, dict)
    assert isinstance(ctx.parts, list)

    ctx = client.get_context("nonexistent")
    assert ctx is None

    client.close()


# --- Prompt via flow ---

def test_prompt_via_flow(daemon, http):
    _, kp, _ = daemon
    kp.put(content="# Test\nA test.\n\n## Prompt\nHello {get}", id=".prompt/agent/test-render")
    r = http.post("/v1/flow", json={
        "state": "prompt", "params": {"name": "test-render"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "done"
    assert "text" in body.get("data", {})
    assert len(body["data"]["text"]) > 0


def test_prompt_not_found_via_flow(http):
    r = http.post("/v1/flow", json={
        "state": "prompt", "params": {"name": "nonexistent-prompt"},
    })
    assert r.status_code == 200
    assert r.json()["status"] == "error"
