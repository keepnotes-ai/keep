"""Tests for the thin CLI renderers and HTTP round-trip."""

import http.client
import json

import pytest

from keep.api import Keeper
from keep.daemon_server import DaemonServer
from keep.thin_cli import (
    _render_context,
    _render_find,
    _render_item_line,
    _render_tags_block,
    _display_tags,
    _truncate,
    _date,
)


# ---------------------------------------------------------------------------
# Renderer unit tests (no daemon needed)
# ---------------------------------------------------------------------------

def test_truncate():
    assert _truncate("short", 100) == "short"
    assert _truncate("a" * 50, 20) == "a" * 17 + "..."
    assert _truncate("line one\nline two\nline three", 100) == "line one line two line three"


def test_date():
    from keep.types import local_date
    assert _date({"_updated": "2026-03-26T12:00:00"}) == local_date("2026-03-26T12:00:00")
    assert _date({"_created": "2026-01-01T00:00:00"}) == local_date("2026-01-01T00:00:00")
    assert _date({}) == ""


def test_display_tags():
    tags = {
        "topic": "cache",
        "status": "open",
        "_created": "2026-03-26",
        "_content_type": "text/plain",
        "_tk::topic": "true",
        "_focus_part": "3",
        "_accessed_date": "2026-03-26",
    }
    display = _display_tags(tags)
    assert "topic" in display
    assert "status" in display
    assert "_created" in display  # shown (matches old CLI)
    assert "_content_type" in display  # shown (matches old CLI)
    assert "_tk::topic" not in display  # always hidden
    assert "_focus_part" not in display  # internal rendering tag
    assert "_accessed_date" not in display  # internal date index


def test_render_tags_block():
    block = _render_tags_block({"topic": "cache", "status": "open"})
    assert 'topic: "cache"' in block
    assert 'status: "open"' in block


def test_render_item_line():
    item = {"id": "test-1", "score": 0.95, "tags": {"_updated": "2026-03-26T12:00:00"}, "summary": "A test item"}
    line = _render_item_line(item, 80)
    assert "test-1" in line
    assert "(0.95)" in line
    assert "2026-03-26" in line
    assert "A test item" in line


def test_render_context_minimal():
    data = {
        "item": {"id": "test-1", "summary": "Test summary", "tags": {"topic": "cache"}},
        "viewing_offset": 0,
        "similar": [],
        "meta": {},
        "edges": {},
        "parts": [],
        "prev": [],
        "next": [],
    }
    output = _render_context(data)
    assert "---" in output
    assert "id: test-1" in output
    assert 'topic: "cache"' in output
    assert "Test summary" in output


def test_render_context_with_similar():
    data = {
        "item": {"id": "test-1", "summary": "Test", "tags": {}},
        "viewing_offset": 0,
        "similar": [{"id": "sim-1", "score": 0.91, "date": "2026-03-25", "summary": "Similar item"}],
        "meta": {},
        "edges": {},
        "parts": [],
        "prev": [],
        "next": [],
    }
    output = _render_context(data)
    assert "similar:" in output
    assert "sim-1" in output
    assert "(0.91)" in output


def test_render_context_with_version():
    data = {
        "item": {"id": "test-1", "summary": "Old version", "tags": {}},
        "viewing_offset": 2,
        "similar": [],
        "meta": {},
        "edges": {},
        "parts": [],
        "prev": [{"offset": 3, "date": "2026-03-20", "summary": "Even older"}],
        "next": [{"offset": 1, "date": "2026-03-24", "summary": "Newer"}],
    }
    output = _render_context(data)
    assert "id: test-1@V{2}" in output
    assert "prev:" in output
    assert "@V{3}" in output
    assert "next:" in output
    assert "@V{1}" in output


def test_render_find():
    data = {
        "notes": [
            {"id": "r-1", "score": 0.95, "tags": {"_updated": "2026-03-26T12:00:00"}, "summary": "First result"},
            {"id": "r-2", "score": 0.88, "tags": {}, "summary": "Second result"},
        ],
    }
    output = _render_find(data)
    assert "r-1" in output
    assert "r-2" in output
    assert "(0.95)" in output


def test_render_find_with_deep_groups():
    data = {
        "notes": [
            {"id": "r-1", "score": 0.95, "tags": {}, "summary": "Primary"},
        ],
        "deep_groups": [
            {"id": "r-1", "items": [
                {"id": "deep-1", "score": 0.72, "tags": {}, "summary": "Deep evidence"},
            ]},
        ],
    }
    output = _render_find(data)
    assert "r-1" in output
    assert "deep-1" in output


def test_render_find_empty():
    assert _render_find({"notes": []}) == "No results."


# ---------------------------------------------------------------------------
# HTTP round-trip with daemon
# ---------------------------------------------------------------------------

@pytest.fixture
def daemon(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    server = DaemonServer(kp, port=0)
    port = server.start()
    yield server, kp, port
    server.stop()
    kp.close()


def test_thin_cli_context_round_trip(daemon):
    """Put via HTTP, get context via /context, render."""
    server, _, port = daemon
    auth = {"Authorization": f"Bearer {server.auth_token}"}

    # Put
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    body = json.dumps({"content": "round trip context test", "id": "rt-ctx"})
    h = {"Content-Type": "application/json", **auth}
    conn.request("POST", "/v1/notes", body, h)
    resp = conn.getresponse()
    resp.read()
    conn.close()
    assert resp.status == 200

    # Get context
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", "/v1/notes/rt-ctx/context?similar_limit=2", headers=auth)
    resp = conn.getresponse()
    data = json.loads(resp.read())
    conn.close()
    assert resp.status == 200

    # Render
    output = _render_context(data)
    assert "id: rt-ctx" in output
    assert "round trip context test" in output
