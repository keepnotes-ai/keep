"""
Tests for the MCP stdio server tool functions.

Tests the tool layer in isolation by mocking Keeper — verifies parameter
mapping, return formatting, and edge cases for all 8 tools.
"""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from keep.types import Item, ItemContext, PromptResult, PromptInfo, SimilarRef, PartRef, VersionRef


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_item(id="test-id", summary="Test summary", tags=None,
               score=None, changed=None):
    """Create a test Item."""
    return Item(
        id=id,
        summary=summary,
        tags=tags or {"_updated_date": "2026-02-20"},
        score=score,
        changed=changed,
    )


def _make_context(item=None):
    """Create a test ItemContext."""
    if item is None:
        item = _make_item()
    return ItemContext(item=item)


@pytest.fixture
def mock_keeper():
    """Mock Keeper instance with default return values."""
    keeper = MagicMock()
    keeper.put.return_value = _make_item(changed=True)
    keeper.find.return_value = []
    keeper.get_context.return_value = None
    keeper.set_now.return_value = _make_item(id="now")
    keeper.tag.return_value = None
    keeper.delete.return_value = False
    keeper.list_items.return_value = []
    keeper.move.return_value = _make_item(id="my-topic")
    keeper.analyze.return_value = []
    return keeper


@pytest.fixture(autouse=True)
def patch_keeper(mock_keeper):
    """Patch _get_keeper to return the mock for all tests."""
    import keep.mcp as mcp_mod
    mcp_mod._keeper = mock_keeper
    yield
    mcp_mod._keeper = None


# ---------------------------------------------------------------------------
# keep_put
# ---------------------------------------------------------------------------

class TestKeepPut:

    @pytest.mark.asyncio
    async def test_put_inline_text(self, mock_keeper):
        from keep.mcp import keep_put
        from keep.api import _text_content_id
        mock_keeper.put.return_value = _make_item(id="%a1b2c3", changed=True)
        result = await keep_put("Hello world")
        assert result == "Stored: %a1b2c3"
        mock_keeper.put.assert_called_once_with(
            "Hello world", id=_text_content_id("Hello world"), summary=None, tags=None,
        )

    @pytest.mark.asyncio
    async def test_put_with_id_and_tags(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(id="my-note", changed=True)
        result = await keep_put(
            "some content", id="my-note", tags={"topic": "test"},
        )
        assert result == "Stored: my-note"
        mock_keeper.put.assert_called_once_with(
            "some content", id="my-note", summary=None, tags={"topic": "test"},
        )

    @pytest.mark.asyncio
    async def test_put_uri_http(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(
            id="https://example.com/doc", changed=True,
        )
        result = await keep_put("https://example.com/doc")
        assert result == "Stored: https://example.com/doc"
        mock_keeper.put.assert_called_once_with(
            uri="https://example.com/doc", id=None, summary=None, tags=None,
        )

    @pytest.mark.asyncio
    async def test_put_uri_file(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(
            id="file:///tmp/doc.md", changed=True,
        )
        result = await keep_put("file:///tmp/doc.md")
        mock_keeper.put.assert_called_once_with(
            uri="file:///tmp/doc.md", id=None, summary=None, tags=None,
        )

    @pytest.mark.asyncio
    async def test_put_unchanged(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(id="x", changed=False)
        result = await keep_put("same content", id="x")
        assert result == "Unchanged: x"

    @pytest.mark.asyncio
    async def test_put_with_summary(self, mock_keeper):
        from keep.mcp import keep_put
        from keep.api import _text_content_id
        mock_keeper.put.return_value = _make_item(id="x", changed=True)
        await keep_put("content", summary="My summary")
        mock_keeper.put.assert_called_once_with(
            "content", id=_text_content_id("content"), summary="My summary", tags=None,
        )

    @pytest.mark.asyncio
    async def test_put_with_analyze(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(id="%abc", changed=True)
        mock_keeper.analyze.return_value = [MagicMock()] * 5
        result = await keep_put("long content", analyze=True)
        assert result == "Stored: %abc (5 parts)"
        mock_keeper.analyze.assert_called_once_with("%abc")

    @pytest.mark.asyncio
    async def test_put_analyze_not_called_when_false(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(changed=True)
        await keep_put("content", analyze=False)
        mock_keeper.analyze.assert_not_called()

    @pytest.mark.asyncio
    async def test_put_error_returns_string(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.side_effect = ValueError("content and uri are mutually exclusive")
        result = await keep_put("bad input")
        assert result.startswith("Error: ")
        assert "mutually exclusive" in result

    @pytest.mark.asyncio
    async def test_put_analyze_error_partial_success(self, mock_keeper):
        from keep.mcp import keep_put
        mock_keeper.put.return_value = _make_item(id="%abc", changed=True)
        mock_keeper.analyze.side_effect = ValueError("content too short")
        result = await keep_put("short", analyze=True)
        assert "Stored: %abc" in result
        assert "analyze failed" in result


# ---------------------------------------------------------------------------
# keep_find
# ---------------------------------------------------------------------------

class TestKeepFind:

    @pytest.mark.asyncio
    async def test_find_no_results(self, mock_keeper):
        from keep.mcp import keep_find
        mock_keeper.find.return_value = []
        result = await keep_find("nonexistent")
        assert result == "No results found."

    @pytest.mark.asyncio
    async def test_find_with_results(self, mock_keeper):
        from keep.mcp import keep_find
        mock_keeper.find.return_value = [
            _make_item(id="%a1b2", summary="Dark mode preference", score=0.82,
                       tags={"_updated_date": "2026-02-20"}),
            _make_item(id="prefs", summary="UI preferences", score=0.71,
                       tags={"_updated_date": "2026-02-18"}),
        ]
        mock_keeper.list_parts.return_value = []
        mock_keeper.list_versions.return_value = []
        result = await keep_find("preferences")
        assert "%a1b2" in result
        assert "(0.82)" in result
        assert "Dark mode preference" in result
        assert "prefs" in result
        assert "(0.71)" in result

    @pytest.mark.asyncio
    async def test_find_passes_params(self, mock_keeper):
        from keep.mcp import keep_find
        mock_keeper.find.return_value = []
        await keep_find(
            "query", tags={"topic": "x"},
            since="P3D", until="2026-02-20",
        )
        mock_keeper.find.assert_called_once_with(
            "query", tags={"topic": "x"}, limit=80,  # 4000 // 50
            since="P3D", until="2026-02-20", deep=False,
        )

    @pytest.mark.asyncio
    async def test_find_no_score(self, mock_keeper):
        from keep.mcp import keep_find
        mock_keeper.find.return_value = [
            _make_item(id="x", summary="No score item", score=None),
        ]
        mock_keeper.list_parts.return_value = []
        mock_keeper.list_versions.return_value = []
        result = await keep_find("test")
        assert "(0." not in result
        assert "- x" in result


# ---------------------------------------------------------------------------
# keep_get
# ---------------------------------------------------------------------------

class TestKeepGet:

    @pytest.mark.asyncio
    async def test_get_not_found(self, mock_keeper):
        from keep.mcp import keep_get
        mock_keeper.get_context.return_value = None
        result = await keep_get("nonexistent")
        assert result == "Not found: nonexistent"

    @pytest.mark.asyncio
    async def test_get_found(self, mock_keeper):
        from keep.mcp import keep_get
        ctx = _make_context(_make_item(id="test-item", summary="A note"))
        mock_keeper.get_context.return_value = ctx
        result = await keep_get("test-item")
        # render_context returns YAML frontmatter
        assert "test-item" in result
        assert "---" in result

    @pytest.mark.asyncio
    async def test_get_passes_id(self, mock_keeper):
        from keep.mcp import keep_get
        mock_keeper.get_context.return_value = None
        await keep_get("my-id")
        mock_keeper.get_context.assert_called_once_with("my-id")


# ---------------------------------------------------------------------------
# keep_now
# ---------------------------------------------------------------------------

class TestKeepNow:

    @pytest.mark.asyncio
    async def test_now_basic(self, mock_keeper):
        from keep.mcp import keep_now
        mock_keeper.set_now.return_value = _make_item(id="now")
        result = await keep_now("Working on MCP implementation")
        assert result == "Context updated: now"
        mock_keeper.set_now.assert_called_once_with(
            "Working on MCP implementation", tags=None,
        )

    @pytest.mark.asyncio
    async def test_now_with_tags(self, mock_keeper):
        from keep.mcp import keep_now
        mock_keeper.set_now.return_value = _make_item(id="now")
        await keep_now("context", tags={"project": "keep"})
        mock_keeper.set_now.assert_called_once_with(
            "context", tags={"project": "keep"},
        )


# ---------------------------------------------------------------------------
# keep_tag
# ---------------------------------------------------------------------------

class TestKeepTag:

    @pytest.mark.asyncio
    async def test_tag_not_found(self, mock_keeper):
        from keep.mcp import keep_tag
        mock_keeper.tag.return_value = None
        result = await keep_tag("nonexistent", {"topic": "x"})
        assert result == "Not found: nonexistent"

    @pytest.mark.asyncio
    async def test_tag_set(self, mock_keeper):
        from keep.mcp import keep_tag
        mock_keeper.tag.return_value = _make_item(id="%abc")
        result = await keep_tag("%abc", {"topic": "preferences"})
        assert "Tagged %abc" in result
        assert "set topic=preferences" in result

    @pytest.mark.asyncio
    async def test_tag_remove(self, mock_keeper):
        from keep.mcp import keep_tag
        mock_keeper.tag.return_value = _make_item(id="%abc")
        result = await keep_tag("%abc", {"old_tag": ""})
        assert "Tagged %abc" in result
        assert "removed old_tag" in result

    @pytest.mark.asyncio
    async def test_tag_set_and_remove(self, mock_keeper):
        from keep.mcp import keep_tag
        mock_keeper.tag.return_value = _make_item(id="%abc")
        result = await keep_tag("%abc", {"topic": "new", "old": ""})
        assert "set topic=new" in result
        assert "removed old" in result

    @pytest.mark.asyncio
    async def test_tag_passes_params(self, mock_keeper):
        from keep.mcp import keep_tag
        mock_keeper.tag.return_value = _make_item()
        await keep_tag("x", {"a": "b"})
        mock_keeper.tag.assert_called_once_with("x", {"a": "b"})


# ---------------------------------------------------------------------------
# keep_delete
# ---------------------------------------------------------------------------

class TestKeepDelete:

    @pytest.mark.asyncio
    async def test_delete_success(self, mock_keeper):
        from keep.mcp import keep_delete
        mock_keeper.delete.return_value = True
        result = await keep_delete("%abc")
        assert result == "Deleted: %abc"

    @pytest.mark.asyncio
    async def test_delete_not_found(self, mock_keeper):
        from keep.mcp import keep_delete
        mock_keeper.delete.return_value = False
        result = await keep_delete("nonexistent")
        assert result == "Not found: nonexistent"


# ---------------------------------------------------------------------------
# keep_list
# ---------------------------------------------------------------------------

class TestKeepList:

    @pytest.mark.asyncio
    async def test_list_empty(self, mock_keeper):
        from keep.mcp import keep_list
        mock_keeper.list_items.return_value = []
        result = await keep_list()
        assert result == "No items found."

    @pytest.mark.asyncio
    async def test_list_with_items(self, mock_keeper):
        from keep.mcp import keep_list
        mock_keeper.list_items.return_value = [
            _make_item(id="now", summary="Current context",
                       tags={"_updated_date": "2026-02-23"}),
            _make_item(id="%a1b2", summary="Dark mode",
                       tags={"_updated_date": "2026-02-20"}),
        ]
        result = await keep_list()
        lines = result.split("\n")
        assert len(lines) == 2
        assert "- now" in lines[0]
        assert "Current context" in lines[0]
        assert "- %a1b2" in lines[1]

    @pytest.mark.asyncio
    async def test_list_passes_params(self, mock_keeper):
        from keep.mcp import keep_list
        mock_keeper.list_items.return_value = []
        await keep_list(
            prefix=".tag/*", tags={"topic": "x"},
            since="P7D", until="2026-02-20", limit=5,
        )
        mock_keeper.list_items.assert_called_once_with(
            prefix=".tag/*", tags={"topic": "x"},
            since="P7D", until="2026-02-20", limit=5,
        )


# ---------------------------------------------------------------------------
# keep_move
# ---------------------------------------------------------------------------

class TestKeepMove:

    @pytest.mark.asyncio
    async def test_move_basic(self, mock_keeper):
        from keep.mcp import keep_move
        mock_keeper.move.return_value = _make_item(id="my-topic")
        result = await keep_move("my-topic")
        assert result == "Moved to: my-topic"
        mock_keeper.move.assert_called_once_with(
            "my-topic", source_id="now", tags=None, only_current=False,
        )

    @pytest.mark.asyncio
    async def test_move_with_source(self, mock_keeper):
        from keep.mcp import keep_move
        mock_keeper.move.return_value = _make_item(id="dest")
        await keep_move("dest", source_id="other")
        mock_keeper.move.assert_called_once_with(
            "dest", source_id="other", tags=None, only_current=False,
        )

    @pytest.mark.asyncio
    async def test_move_with_tags_and_only_current(self, mock_keeper):
        from keep.mcp import keep_move
        mock_keeper.move.return_value = _make_item(id="dest")
        await keep_move(
            "dest", tags={"topic": "x"}, only_current=True,
        )
        mock_keeper.move.assert_called_once_with(
            "dest", source_id="now", tags={"topic": "x"}, only_current=True,
        )

    @pytest.mark.asyncio
    async def test_move_error_returns_string(self, mock_keeper):
        from keep.mcp import keep_move
        mock_keeper.move.side_effect = ValueError("source item 'nonexistent' not found")
        result = await keep_move("dest", source_id="nonexistent")
        assert result.startswith("Error: ")
        assert "not found" in result


# ---------------------------------------------------------------------------
# keep_prompt
# ---------------------------------------------------------------------------

class TestKeepPrompt:

    @pytest.mark.asyncio
    async def test_prompt_list_when_no_name(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.list_prompts.return_value = [
            PromptInfo(name="reflect", summary="Reflection practice"),
            PromptInfo(name="session-start", summary="Session context"),
        ]
        result = await keep_prompt()
        assert "reflect" in result
        assert "session-start" in result
        assert "Reflection practice" in result

    @pytest.mark.asyncio
    async def test_prompt_list_empty(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.list_prompts.return_value = []
        result = await keep_prompt()
        assert result == "No agent prompts available."

    @pytest.mark.asyncio
    async def test_prompt_not_found(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.render_prompt.return_value = None
        result = await keep_prompt(name="nonexistent")
        assert result == "Prompt not found: nonexistent"

    @pytest.mark.asyncio
    async def test_prompt_renders_with_context(self, mock_keeper):
        from keep.mcp import keep_prompt
        ctx = _make_context(_make_item(id="now", summary="Working on MCP"))
        mock_keeper.render_prompt.return_value = PromptResult(
            context=ctx,
            search_results=None,
            prompt="Reflect on:\n\n{get}\n\nDo the thing.",
        )
        result = await keep_prompt(name="reflect")
        assert "now" in result
        assert "Do the thing." in result
        assert "{get}" not in result

    @pytest.mark.asyncio
    async def test_prompt_renders_with_find(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.render_prompt.return_value = PromptResult(
            context=None,
            search_results=[
                _make_item(id="%abc", summary="A learning", score=0.85,
                           tags={"_created": "2026-02-20T10:00:00",
                                 "_updated": "2026-02-20T10:00:00",
                                 "_updated_date": "2026-02-20"}),
            ],
            prompt="Context:\n\n{get}\n{find}\n\nReflect.",
        )
        result = await keep_prompt(name="reflect", text="learnings")
        assert "%abc" in result
        assert "2026-02-20" in result
        assert "A learning" in result
        assert "{find}" not in result

    @pytest.mark.asyncio
    async def test_prompt_passes_params(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.render_prompt.return_value = PromptResult(
            context=None, search_results=None, prompt="Hello",
        )
        await keep_prompt(
            name="reflect", text="query", id="my-id",
            tags={"project": "x"}, since="P7D", until="2026-02-20",
        )
        mock_keeper.render_prompt.assert_called_once_with(
            "reflect", "query", id="my-id",
            since="P7D", until="2026-02-20", tags={"project": "x"},
            deep=False, token_budget=None,
        )

    @pytest.mark.asyncio
    async def test_prompt_expands_text_since_until(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.render_prompt.return_value = PromptResult(
            context=None,
            search_results=None,
            prompt="Q: {text}\nSince: {since}\nUntil: {until}",
            text="what happened",
            since="P7D",
            until="2026-02-20",
        )
        result = await keep_prompt(name="query", text="what happened")
        assert "Q: what happened" in result
        assert "Since: P7D" in result
        assert "Until: 2026-02-20" in result
        assert "{text}" not in result

    @pytest.mark.asyncio
    async def test_prompt_find_shows_date(self, mock_keeper):
        from keep.mcp import keep_prompt
        mock_keeper.render_prompt.return_value = PromptResult(
            context=None,
            search_results=[
                _make_item(id="%abc", summary="Old memory", score=0.9,
                           tags={"_created": "2023-10-15T12:00:00",
                                 "_updated_date": "2026-02-20"}),
            ],
            prompt="{find}",
        )
        result = await keep_prompt(name="query", text="test")
        assert "2023-10-15" in result  # _created date, not _updated_date
        assert "%abc" in result


# ---------------------------------------------------------------------------
# Integration: asyncio.Lock serialization
# ---------------------------------------------------------------------------

class TestSerialization:

    @pytest.mark.asyncio
    async def test_concurrent_calls_are_serialized(self, mock_keeper):
        """Verify the asyncio.Lock prevents concurrent Keeper access."""
        from keep.mcp import keep_put, keep_find, _lock

        call_order = []

        original_put = mock_keeper.put
        original_find = mock_keeper.find

        def slow_put(*args, **kwargs):
            call_order.append("put_start")
            call_order.append("put_end")
            return _make_item(changed=True)

        def slow_find(*args, **kwargs):
            call_order.append("find_start")
            call_order.append("find_end")
            return []

        mock_keeper.put.side_effect = slow_put
        mock_keeper.find.side_effect = slow_find

        await asyncio.gather(
            keep_put("test content"),
            keep_find("test query"),
        )

        # Both operations completed
        assert "put_start" in call_order
        assert "find_start" in call_order
        # Due to the lock, one must fully complete before the other starts
        put_start = call_order.index("put_start")
        put_end = call_order.index("put_end")
        find_start = call_order.index("find_start")
        find_end = call_order.index("find_end")
        assert (put_end < find_start) or (find_end < put_start)


# ---------------------------------------------------------------------------
# Startup hints: _check_mcp_setup
# ---------------------------------------------------------------------------

class TestCheckMcpSetup:

    def test_no_tools_detected(self, tmp_path, capsys, monkeypatch):
        """No hints when no tool config dirs exist."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        assert capsys.readouterr().err == ""

    def test_claude_code_not_configured(self, tmp_path, capsys, monkeypatch):
        """Hint shown when ~/.claude exists but mcpServers.keep missing."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        (tmp_path / ".claude").mkdir()
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        err = capsys.readouterr().err
        assert "Claude Code" in err
        assert "claude mcp add" in err

    def test_claude_code_already_configured(self, tmp_path, capsys, monkeypatch):
        """No hint when ~/.claude.json has mcpServers.keep."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        (tmp_path / ".claude").mkdir()
        import json
        (tmp_path / ".claude.json").write_text(json.dumps({
            "mcpServers": {"keep": {"command": "keep", "args": ["mcp"]}}
        }))
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        assert "Claude Code" not in capsys.readouterr().err

    def test_kiro_not_configured(self, tmp_path, capsys, monkeypatch):
        """Hint shown when ~/.kiro exists but mcpServers.keep missing."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        (tmp_path / ".kiro").mkdir()
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        err = capsys.readouterr().err
        assert "Kiro" in err
        assert "kiro-cli mcp add" in err

    def test_kiro_already_configured(self, tmp_path, capsys, monkeypatch):
        """No hint when ~/.kiro/settings/mcp.json has mcpServers.keep."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        kiro_dir = tmp_path / ".kiro" / "settings"
        kiro_dir.mkdir(parents=True)
        import json
        (kiro_dir / "mcp.json").write_text(json.dumps({
            "mcpServers": {"keep": {"command": "keep", "args": ["mcp"]}}
        }))
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        assert "Kiro" not in capsys.readouterr().err

    def test_codex_not_configured(self, tmp_path, capsys, monkeypatch):
        """Hint shown when ~/.codex exists but mcp_servers.keep missing."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        (tmp_path / ".codex").mkdir()
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        err = capsys.readouterr().err
        assert "Codex" in err
        assert "codex mcp add" in err

    def test_codex_already_configured(self, tmp_path, capsys, monkeypatch):
        """No hint when ~/.codex/config.toml has [mcp_servers.keep]."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        codex_dir = tmp_path / ".codex"
        codex_dir.mkdir()
        (codex_dir / "config.toml").write_text(
            '[mcp_servers.keep]\ncommand = "keep"\nargs = ["mcp"]\n'
        )
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        assert "Codex" not in capsys.readouterr().err

    def test_vscode_not_configured(self, tmp_path, capsys, monkeypatch):
        """Hint shown when VS Code user dir exists but mcp.json missing."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        import platform
        if platform.system() == "Darwin":
            vscode_dir = tmp_path / "Library" / "Application Support" / "Code"
        else:
            vscode_dir = tmp_path / ".config" / "Code"
        vscode_dir.mkdir(parents=True)
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        err = capsys.readouterr().err
        assert "VS Code" in err
        assert "code --add-mcp" in err

    def test_vscode_already_configured(self, tmp_path, capsys, monkeypatch):
        """No hint when VS Code mcp.json has servers.keep."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        import platform
        if platform.system() == "Darwin":
            vscode_dir = tmp_path / "Library" / "Application Support" / "Code"
        else:
            vscode_dir = tmp_path / ".config" / "Code"
        user_dir = vscode_dir / "User"
        user_dir.mkdir(parents=True)
        import json
        (user_dir / "mcp.json").write_text(json.dumps({
            "servers": {"keep": {"command": "keep", "args": ["mcp"]}}
        }))
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        assert "VS Code" not in capsys.readouterr().err

    def test_multiple_tools_detected(self, tmp_path, capsys, monkeypatch):
        """Hints for all unconfigured tools in one message."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".kiro").mkdir()
        (tmp_path / ".codex").mkdir()
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        err = capsys.readouterr().err
        assert "Claude Code" in err
        assert "Kiro" in err
        assert "Codex" in err

    def test_corrupt_settings_json_ignored(self, tmp_path, capsys, monkeypatch):
        """Corrupt JSON doesn't crash, just shows hint."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".claude.json").write_text("not json{{{")
        from keep.mcp import _check_mcp_setup
        _check_mcp_setup()
        err = capsys.readouterr().err
        assert "Claude Code" in err  # hint still shown
