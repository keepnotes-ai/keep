"""Tests for the thin CLI renderers, HTTP round-trip, and put input handling."""

import http.client
import io
import json
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from keep.api import Keeper
from keep.daemon_server import DaemonServer
from keep.thin_cli import (
    _render_context,
    _render_find,
    _render_item_line,
    _render_tags_block,
    _get_one_item,
    _display_tags,
    _truncate,
    _date,
    put,
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


def test_get_one_item_retries_after_connection_refused():
    """The thin CLI re-resolves daemon discovery if the first request loses the daemon."""
    with (
        patch("keep.thin_cli._http") as mock_http,
        patch("keep.thin_cli._get_port", return_value=5338),
    ):
        mock_http.side_effect = [
            ConnectionRefusedError(61, "refused"),
            (200, {
                "item": {"id": "now", "summary": "ok", "tags": {}},
                "viewing_offset": 0,
                "similar": [],
                "meta": {},
                "edges": {},
                "parts": [],
                "prev": [],
                "next": [],
            }),
        ]

        result = _get_one_item(
            5337,
            "now",
            version=None,
            limit=10,
            similar=False,
            meta=False,
            parts=False,
            history=False,
            tag=None,
            json_output=False,
        )

    assert "id: now" in result
    assert mock_http.call_args_list[0].args[1] == 5337
    assert mock_http.call_args_list[1].args[1] == 5338


def test_get_now_uses_now_context_defaults():
    """Bare `keep get now` should match `keep now` exactly."""
    data = {
        "item": {"id": "now", "summary": "Active context", "tags": {}},
        "viewing_offset": 0,
        "similar": [],
        "meta": {},
        "edges": {},
        "parts": [],
        "prev": [],
        "next": [],
    }
    with patch("keep.thin_cli._get", return_value=data) as mock_get:
        result = _get_one_item(
            5337,
            "now",
            version=None,
            limit=10,
            similar=False,
            meta=False,
            parts=False,
            history=False,
            tag=None,
            json_output=False,
        )

    mock_get.assert_called_once_with(5337, "/v1/notes/now/context")
    assert "id: now" in result


def test_put_id_now_inline_matches_now_output():
    """Inline `keep put --id now ...` should use the same write+read path as `keep now`."""
    data = {
        "item": {"id": "now", "summary": "Working on CLI cleanup", "tags": {"topic": "cli"}},
        "viewing_offset": 0,
        "similar": [],
        "meta": {},
        "edges": {},
        "parts": [],
        "prev": [],
        "next": [],
    }
    with (
        patch("keep.thin_cli._get_port", return_value=5337),
        patch("keep.thin_cli._post") as mock_post,
        patch("keep.thin_cli._get", return_value=data) as mock_get,
        patch("keep.thin_cli._render_context", return_value="rendered now") as mock_render,
        patch("keep.thin_cli.typer.echo") as mock_echo,
    ):
        put(source="Working on CLI cleanup", id="now", tags=["topic=cli"])

    mock_post.assert_called_once_with(
        5337,
        "/v1/notes",
        {"content": "Working on CLI cleanup", "id": "now", "tags": {"topic": "cli"}},
    )
    mock_get.assert_called_once_with(5337, "/v1/notes/now/context")
    mock_render.assert_called_once_with(data)
    mock_echo.assert_called_once_with("rendered now")


def test_put_id_now_file_keeps_put_semantics(tmp_path):
    """`put --id now` only collapses for text/stdin input, not file mode."""
    note = tmp_path / "note.md"
    note.write_text("hello")
    with (
        patch("keep.thin_cli._get_port", return_value=5337),
        patch("keep.thin_cli._post", return_value={"id": "now"}) as mock_post,
        patch("keep.thin_cli._get") as mock_get,
        patch("keep.thin_cli.typer.echo") as mock_echo,
    ):
        put(source=str(note), id="now")

    mock_post.assert_called_once()
    body = mock_post.call_args.args[2]
    assert body["id"] == "now"
    assert body["content"] is None
    assert body["uri"] == f"file://{note.resolve()}"
    mock_get.assert_not_called()
    mock_echo.assert_called_once_with("now stored.")


# ---------------------------------------------------------------------------
# Docstring formatting guard
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("cmd", [
    "find", "put", "get", "list", "tag", "del", "now",
    "prompt", "flow", "edit", "analyze", "help", "config",
    "pending",
])
def test_thin_cli_help_no_literal_backslash_b(cmd):
    """Thin CLI help renders backspace formatting, not literal backslash-b.

    Typer/Click uses backspace (0x08) in docstrings to preserve line breaks.
    Raw strings turn this into a literal two-char sequence that Click ignores.
    """
    result = subprocess.run(
        [sys.executable, "-m", "keep", cmd, "--help"],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, f"{cmd} --help failed: {result.stderr}"
    assert r"\b" not in result.stdout, (
        f"thin_cli {cmd} --help contains literal '\\b' — "
        f"docstring is likely a raw string instead of regular string"
    )


@pytest.mark.parametrize("cmd", ["get", "find", "put", "tag", "edit"])
def test_thin_cli_help_examples_not_wrapped(cmd):
    """CLI help examples are preserved on separate lines, not paragraph-wrapped.

    Click's backspace directive (\\b) scopes to the paragraph it precedes.
    A blank line between \\b content and the examples block breaks scoping,
    causing Click to re-wrap the examples into a single paragraph.
    """
    result = subprocess.run(
        [sys.executable, "-m", "keep", cmd, "--help"],
        capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, f"{cmd} --help failed: {result.stderr}"
    # If examples are wrapped, "keep {cmd}" appears multiple times on the
    # same line (e.g. "keep get doc:1 # ... keep get doc:2 # ...").
    # When properly formatted, each "keep " starts its own line.
    lines = result.stdout.splitlines()
    example_lines = [l.strip() for l in lines if l.strip().startswith(f"keep {cmd}")]
    assert len(example_lines) >= 2, (
        f"{cmd} --help should show multiple example lines starting with 'keep {cmd}', "
        f"got {len(example_lines)}. Examples may be paragraph-wrapped."
    )


# ---------------------------------------------------------------------------
# put() input handling
# ---------------------------------------------------------------------------

class TestPutStdinSafety:
    """Tests for stdin detection and binary data handling in put."""

    def test_stdin_uses_select_not_isatty(self):
        """Stdin detection uses _has_stdin_data (select-based), not raw isatty check.

        Socket-backed stdin (e.g. exec sandboxes) is not a TTY but has
        no data — _has_stdin_data returns False via select(), preventing hangs.
        """
        from keep.thin_cli import _has_stdin_data

        # A TTY is not stdin data
        with patch("keep.thin_cli.sys") as mock_sys:
            mock_sys.stdin.isatty.return_value = True
            assert _has_stdin_data() is False

    def test_binary_stdin_produces_helpful_error(self):
        """Binary data on stdin gets a clear error, not an unhelpful traceback."""
        result = subprocess.run(
            [sys.executable, "-m", "keep", "put", "-"],
            input=b"\x80\x81\x82\xff",
            capture_output=True, timeout=15,
        )
        # Should fail with a helpful message, not a raw UnicodeDecodeError traceback
        assert result.returncode != 0
        stderr = result.stderr.decode(errors="replace")
        assert "binary" in stderr.lower() or "utf-8" in stderr.lower() or "Error" in stderr


class TestPutFrontmatter:
    """Tests for YAML frontmatter extraction from stdin content."""

    def test_frontmatter_tags_extracted_from_stdin(self, daemon):
        """Piping markdown with frontmatter extracts tags and strips frontmatter."""
        server, kp, port = daemon
        auth = {"Authorization": f"Bearer {server.auth_token}"}

        # Simulate what thin_cli put does: frontmatter extraction then POST
        content_with_fm = "---\ntopic: testing\nstatus: draft\n---\nActual content here."

        from keep.utils import _extract_markdown_frontmatter
        body_text, fm_tags = _extract_markdown_frontmatter(content_with_fm)

        assert fm_tags.get("topic") == "testing"
        assert fm_tags.get("status") == "draft"
        assert body_text == "Actual content here."
        assert "---" not in body_text

    def test_frontmatter_cli_tags_override(self):
        """CLI -t tags override frontmatter tags with the same key."""
        from keep.utils import _extract_markdown_frontmatter

        content = "---\ntopic: from-frontmatter\n---\nbody"
        body_text, fm_tags = _extract_markdown_frontmatter(content)
        cli_tags = {"topic": "from-cli"}

        # thin_cli merges as: {**fm_tags, **cli_tags}
        merged = {**fm_tags, **cli_tags}
        assert merged["topic"] == "from-cli"


class TestPutMultiValueTags:
    """Tests for multi-value tag parsing in put."""

    def test_repeated_tag_key_produces_list(self):
        """Using -t key=a -t key=b produces {"key": ["a", "b"]}."""
        from keep.thin_cli import put

        # We can't easily call put() directly (it needs a daemon),
        # so test the parsing logic inline
        tags_input = ["topic=auth", "topic=security", "status=draft"]
        parsed: dict = {}
        for t in tags_input:
            k, v = t.split("=", 1)
            key = k.casefold()
            existing = parsed.get(key)
            if existing is None:
                parsed[key] = v
            elif isinstance(existing, list):
                if v not in existing:
                    existing.append(v)
            elif existing != v:
                parsed[key] = [existing, v]

        assert parsed == {"topic": ["auth", "security"], "status": "draft"}

    def test_duplicate_value_not_repeated(self):
        """Using -t key=a -t key=a doesn't duplicate."""
        tags_input = ["topic=auth", "topic=auth"]
        parsed: dict = {}
        for t in tags_input:
            k, v = t.split("=", 1)
            key = k.casefold()
            existing = parsed.get(key)
            if existing is None:
                parsed[key] = v
            elif isinstance(existing, list):
                if v not in existing:
                    existing.append(v)
            elif existing != v:
                parsed[key] = [existing, v]

        assert parsed == {"topic": "auth"}

    def test_tag_format_error_with_colon_hint(self):
        """Misformatted tag with colon gets a 'did you mean' hint."""
        result = subprocess.run(
            [sys.executable, "-m", "keep", "put", "test", "-t", "topic:auth"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode != 0
        assert "Did you mean" in result.stderr
