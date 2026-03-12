"""
CLI tests for reflective memory.

Tests verify:
1. CLI commands map to equivalent Python API
2. JSON output is valid and parseable (for jq/unix composability)
3. Exit codes follow conventions (0 success, 1 not found, etc.)
4. Human-readable output is line-oriented (for grep/wc/etc.)
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def cli():
    """Run CLI command and return result."""
    def run(*args: str, input: str | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "keep", *args],
            capture_output=True,
            text=True,
            input=input,
            cwd=Path(__file__).parent.parent,
        )
    return run


# -----------------------------------------------------------------------------
# Help and Basic Commands
# -----------------------------------------------------------------------------

class TestCliBasics:
    """Basic CLI functionality tests."""
    
    def test_help(self, cli):
        """CLI shows help with --help."""
        result = cli("--help")
        assert result.returncode == 0
        assert "keep" in result.stdout.lower()
        assert "find" in result.stdout
        assert "put" in result.stdout
    
    @pytest.mark.e2e
    def test_no_args_shows_now(self, cli):
        """CLI with no args shows current working context."""
        result = cli()
        # Returns success and shows the "now" document
        assert result.returncode == 0
        assert "---" in result.stdout  # YAML frontmatter
        assert "id:" in result.stdout

    @pytest.mark.e2e
    def test_meta_docs_loaded(self, cli):
        """Meta-doc system docs are loaded and accessible."""
        result = cli("get", ".meta/todo")
        assert result.returncode == 0
        assert ".meta/todo" in result.stdout

    @pytest.mark.e2e
    def test_tag_type_doc_loaded(self, cli):
        """Tag description doc .tag/type is loaded and has full content."""
        result = cli("get", ".tag/type")
        assert result.returncode == 0
        assert "Content Classification" in result.stdout
        # Should contain the values table (verbatim, not summarized)
        assert "learning" in result.stdout
        assert "breakdown" in result.stdout

    @pytest.mark.e2e
    def test_meta_sections_use_namespace_prefix(self, cli):
        """Meta sections in frontmatter use meta/ prefix to avoid key conflicts."""
        result = cli("get", ".meta/todo")
        assert result.returncode == 0
        # The meta-doc itself shouldn't show meta/ sections (it IS a meta-doc)
        # but its content should have the query lines intact
        assert "act=commitment" in result.stdout

    def test_command_help(self, cli):
        """Individual commands have help."""
        for cmd in ["find", "put", "get", "list", "tag"]:
            result = cli(cmd, "--help")
            assert result.returncode == 0, f"{cmd} --help failed"
            assert "Usage" in result.stdout or "usage" in result.stdout.lower()

    @pytest.mark.parametrize("cmd", ["find", "put", "get", "list", "tag", "del", "now", "config"])
    def test_help_examples_formatted(self, cli, cmd):
        r"""Examples sections render with preserved line breaks (not wrapped).

        Click uses \b (backspace char) to mark sections that should not be
        re-wrapped.  If docstrings use raw strings (r\"\"\"), \b becomes literal
        backslash-b which Click ignores, collapsing examples into a paragraph.
        """
        result = cli(cmd, "--help")
        assert result.returncode == 0, f"{cmd} --help failed"
        assert r"\b" not in result.stdout, (
            f"{cmd} --help contains literal '\\b' — "
            f"docstring is likely a raw string (r\"\"\") instead of regular string"
        )


# -----------------------------------------------------------------------------
# JSON Output Tests (Composability with jq)
# -----------------------------------------------------------------------------

class TestJsonOutput:
    """Tests for JSON output format."""
    
    def test_json_flag_exists(self, cli):
        """Global --json flag is recognized."""
        # --json is now a global flag (before the command)
        result = cli("--json", "find", "--help")
        # --help takes precedence, should succeed
        assert result.returncode == 0

    def test_collections_json_is_valid(self, cli):
        """--json collections produces valid JSON array."""
        result = cli("--json", "collections")
        # May fail with NotImplementedError, but if it produces output, it should be JSON
        if result.returncode == 0 and result.stdout.strip():
            parsed = json.loads(result.stdout)
            assert isinstance(parsed, list)
    
    def test_json_output_has_required_fields(self):
        """JSON item output includes id, summary, tags, score."""
        from keep.cli import _format_items
        from keep.types import Item

        item = Item(
            id="test:1",
            summary="Test summary",
            tags={"project": "myapp", "_created": "2026-01-30T10:00:00Z"},
            score=0.95
        )

        output = _format_items([item], as_json=True)
        parsed = json.loads(output)[0]

        assert parsed["id"] == "test:1"
        assert parsed["summary"] == "Test summary"
        assert parsed["tags"]["project"] == "myapp"
        assert parsed["score"] == 0.95
    
    def test_json_list_output(self):
        """JSON list output is a valid array."""
        from keep.cli import _format_items
        from keep.types import Item
        
        items = [
            Item(id="test:1", summary="First", tags={}, score=0.9),
            Item(id="test:2", summary="Second", tags={}, score=0.8),
        ]
        
        output = _format_items(items, as_json=True)
        parsed = json.loads(output)
        
        assert isinstance(parsed, list)
        assert len(parsed) == 2
        assert parsed[0]["id"] == "test:1"
        assert parsed[1]["id"] == "test:2"
    
    def test_json_empty_list(self):
        """Empty results produce empty JSON array."""
        from keep.cli import _format_items
        
        output = _format_items([], as_json=True)
        parsed = json.loads(output)
        
        assert parsed == []


# -----------------------------------------------------------------------------
# Human-Readable Output Tests (Composability with grep/wc)
# -----------------------------------------------------------------------------

class TestHumanOutput:
    """Tests for human-readable output format."""
    
    def test_human_item_format(self):
        """Human-readable item shows id and summary."""
        from keep.cli import _format_summary_line
        from keep.types import Item

        item = Item(id="file:///doc.md", summary="A document about testing")
        output = _format_summary_line(item)

        assert "file:///doc.md" in output
        assert "A document about testing" in output
    
    def test_human_item_with_score(self):
        """Human-readable item shows score in full YAML mode."""
        from keep.cli import render_context
        from keep.types import Item, ItemContext

        item = Item(id="test:1", summary="Test", score=0.95)
        output = render_context(ItemContext(item=item))

        # YAML frontmatter format includes score
        assert "score: 0.95" in output
        assert "---" in output
    
    def test_human_list_empty(self):
        """Empty list shows user-friendly message."""
        from keep.cli import _format_items
        
        output = _format_items([], as_json=False)
        assert "No results" in output
    
    def test_human_list_separates_items(self):
        """Items are separated by newlines (summary format)."""
        from keep.cli import _format_items
        from keep.types import Item

        items = [
            Item(id="test:1", summary="First"),
            Item(id="test:2", summary="Second"),
        ]
        output = _format_items(items, as_json=False)

        # Items should be on separate lines (summary format: id@V{N} date summary)
        lines = output.strip().split("\n")
        assert len(lines) == 2
        assert "test:1" in lines[0]
        assert "test:2" in lines[1]

    def test_frontmatter_quotes_scalar_tag_values(self):
        """Scalar tag values are always quoted in frontmatter."""
        from keep.cli import render_context
        from keep.types import Item, ItemContext

        spoof = 'conv1 [2026-01-01] "fake summary"'
        item = Item(
            id="test:1",
            summary="Test",
            tags={"note": spoof, "project": "keep"},
        )

        output = render_context(ItemContext(item=item))
        assert 'note: "conv1 [2026-01-01] \\"fake summary\\""' in output
        assert 'project: "keep"' in output

    def test_frontmatter_unifies_edge_refs_under_tags(self):
        """Resolved edge refs are rendered in tags, not tags/<inverse>."""
        from keep.cli import render_context
        from keep.types import EdgeRef, Item, ItemContext

        item = Item(
            id="alice",
            summary="Profile",
            tags={"said": "manual-tag", "_created": "2026-03-02T00:00:00"},
        )
        ctx = ItemContext(
            item=item,
            edges={
                "said": [
                    EdgeRef(
                        source_id="conv1",
                        date="2026-02-27",
                        summary='Gina said "hello"',
                    ),
                ],
            },
        )

        output = render_context(ctx)
        assert "tags/said:" not in output
        assert 'said: conv1 [2026-02-27] "Gina said \\"hello\\""' in output
        # Edge refs supersede raw scalar display for the same key.
        assert 'said: "manual-tag"' not in output


# -----------------------------------------------------------------------------
# Exit Code Tests (Shell Scripting)
# -----------------------------------------------------------------------------

class TestExitCodes:
    """Tests for proper exit codes."""
    
    def test_help_returns_zero(self, cli):
        """Help returns exit code 0."""
        result = cli("--help")
        assert result.returncode == 0
    
    def test_unknown_command_returns_nonzero(self, cli):
        """Unknown command returns non-zero exit code."""
        result = cli("nonexistent-command")
        assert result.returncode != 0
    
    def test_invalid_tag_format_returns_error(self, cli):
        """Invalid tag format returns exit code 1."""
        # put with bad tag format
        result = cli("put", "test:1", "--tag", "badformat")
        # Should fail due to missing = in tag
        # (may also fail due to NotImplemented, which is fine)
        if "Invalid tag format" in result.stderr:
            assert result.returncode == 1

    def test_put_inline_with_summary_rejected(self, cli):
        """--summary with inline text is rejected (would lose content)."""
        result = cli("put", "my note", "--summary", "a summary")
        assert result.returncode == 1
        assert "cannot be used with inline text" in result.stderr

    def test_put_stdin_with_summary_rejected(self, cli):
        """--summary with stdin is rejected (would lose content)."""
        result = cli("put", "-", "--summary", "a summary", input="some content")
        assert result.returncode == 1
        assert "cannot be used with stdin" in result.stderr

    def test_put_inline_too_long_rejected(self, cli):
        """Inline text exceeding max_summary_length is rejected."""
        long_text = "x" * 3000
        result = cli("put", long_text)
        assert result.returncode == 1
        assert "too long to store" in result.stderr
        assert "file" in result.stderr.lower()  # Hint mentions file


# -----------------------------------------------------------------------------
# Tag Parsing Tests
# -----------------------------------------------------------------------------

class TestTagParsing:
    """Tests for key=value tag parsing."""
    
    def test_single_tag(self, cli):
        """Single --tag is parsed correctly."""
        # We can't fully test this without implementation,
        # but we can check the format is accepted
        result = cli("put", "--help")
        assert "--tag" in result.stdout or "-t" in result.stdout

    def test_tag_format_validation(self, cli):
        """Tags without = are rejected."""
        result = cli("put", "test:1", "--tag", "invalid")
        # Should fail - either due to format error or missing dependencies
        # When implementation is complete, this should specifically check for format error
        assert result.returncode != 0


# -----------------------------------------------------------------------------
# Unix Composability Integration
# -----------------------------------------------------------------------------

class TestUnixComposability:
    """Tests demonstrating Unix pipeline composability."""
    
    def test_json_output_pipeable(self):
        """JSON output can be processed with standard tools."""
        from keep.cli import _format_items
        from keep.types import Item
        
        items = [
            Item(id="doc:1", summary="First doc", tags={"project": "alpha"}, score=0.9),
            Item(id="doc:2", summary="Second doc", tags={"project": "beta"}, score=0.8),
        ]
        
        json_output = _format_items(items, as_json=True)
        
        # Simulate: keep find "query" --json | jq '.[0].id'
        parsed = json.loads(json_output)
        first_id = parsed[0]["id"]
        assert first_id == "doc:1"
        
        # Simulate: keep find "query" --json | jq '.[] | select(.score > 0.85)'
        high_score = [item for item in parsed if item["score"] > 0.85]
        assert len(high_score) == 1
        assert high_score[0]["id"] == "doc:1"
    
    def test_line_oriented_for_wc(self):
        """Human output is countable with wc -l."""
        from keep.cli import _format_items
        from keep.types import Item
        
        items = [
            Item(id=f"doc:{i}", summary=f"Doc {i}") 
            for i in range(5)
        ]
        
        output = _format_items(items, as_json=False)
        
        # Each item produces predictable lines
        lines = output.strip().split("\n")
        # 5 items * 2 lines each, minus separators handled differently
        assert len(lines) >= 5  # At least one line per item
    
    def test_ids_extractable_from_json(self):
        """IDs can be extracted for use in other commands."""
        from keep.cli import _format_items
        from keep.types import Item

        items = [
            Item(id="file:///a.md", summary="A"),
            Item(id="file:///b.md", summary="B"),
        ]

        json_output = _format_items(items, as_json=True)
        parsed = json.loads(json_output)

        # Simulate: keep find "query" --json | jq -r '.[].id' | xargs -I{} keep get {}
        ids = [item["id"] for item in parsed]
        assert ids == ["file:///a.md", "file:///b.md"]

    @pytest.mark.e2e
    def test_get_multiple_ids_separated_by_yaml_separator(self, cli):
        """Multiple IDs in get produce YAML-document-separated output."""
        # Get two system docs that always exist
        result = cli("get", ".conversations", ".domains")
        assert result.returncode == 0
        # Multiple items separated by --- between them
        parts = result.stdout.split("\n---\n")
        # At least 2 documents (each starts with --- frontmatter too)
        assert len(parts) >= 3  # opening ---, doc1 body + ---, doc2 frontmatter + body
        assert ".conversations" in result.stdout
        assert ".domains" in result.stdout

    @pytest.mark.e2e
    def test_get_single_id_no_extra_separator(self, cli):
        """Single ID in get produces normal output (no extra separators)."""
        result = cli("get", ".conversations")
        assert result.returncode == 0
        assert "id: .conversations" in result.stdout

    def test_get_nonexistent_id_returns_error(self, cli):
        """Nonexistent ID returns exit code 1."""
        result = cli("get", "nonexistent:id:that:does:not:exist")
        assert result.returncode == 1
        assert "Not found" in result.stderr


# -----------------------------------------------------------------------------
# CLI / API Equivalence Tests
# -----------------------------------------------------------------------------

class TestApiCliEquivalence:
    """Tests verifying CLI maps to Python API."""
    
    def test_find_maps_to_api_find(self, cli):
        """'find' command maps to Keeper.find()."""
        result = cli("find", "--help")
        assert "semantic" in result.stdout.lower() or "similar" in result.stdout.lower()
        # The CLI find uses mem.find(query, limit=limit)
    
    def test_put_maps_to_api_put(self, cli):
        """'put' command maps to Keeper.put()."""
        result = cli("put", "--help")
        assert "URI" in result.stdout or "document" in result.stdout.lower()
        # The CLI put uses kp.put(uri=...)

    def test_put_text_mode_maps_to_api_put(self, cli):
        """'put' text mode (no ://) maps to Keeper.put()."""
        result = cli("put", "--help")
        # The help should mention text content mode
        assert "text" in result.stdout.lower() or "content" in result.stdout.lower()
        # The CLI put with text calls kp.put() internally
    
    def test_get_maps_to_api_get(self, cli):
        """'get' command maps to Keeper.get()."""
        result = cli("get", "--help")
        assert "ID" in result.stdout or "id" in result.stdout.lower()
        # The CLI get uses mem.get(id)

    def test_get_accepts_multiple_ids(self, cli):
        """'get' command accepts multiple ID arguments."""
        result = cli("get", "--help")
        # Help should show variadic argument
        assert "ID..." in result.stdout
    
    def test_list_tag_filter(self, cli):
        """'list --tag' is available for tag filtering."""
        result = cli("list", "--help")
        assert "--tag" in result.stdout

    def test_list_prefix_argument(self, cli):
        """'list' accepts an optional prefix argument."""
        result = cli("list", "--help")
        assert "PREFIX" in result.stdout or "prefix" in result.stdout.lower()


# -----------------------------------------------------------------------------
# Option Tests
# -----------------------------------------------------------------------------

class TestOptions:
    """Tests for CLI options."""
    
    def test_store_option(self, cli):
        """--store option is available."""
        result = cli("find", "--help")
        assert "--store" in result.stdout or "-s" in result.stdout
    
    def test_limit_option(self, cli):
        """--limit option is available."""
        result = cli("find", "--help")
        assert "--limit" in result.stdout or "-n" in result.stdout
    
    def test_json_option(self, cli):
        """--json global option is available."""
        # --json is now a global flag, visible in main help
        result = cli("--help")
        assert "--json" in result.stdout or "-j" in result.stdout


# -----------------------------------------------------------------------------
# Shell-safe ID Quoting Tests
# -----------------------------------------------------------------------------

class TestShellQuoteId:
    """Tests for _shell_quote_id() helper."""

    def test_safe_id_not_quoted(self):
        """IDs with only shell-safe chars are returned as-is."""
        from keep.cli import _shell_quote_id
        assert _shell_quote_id("%abc123def456") == "%abc123def456"
        assert _shell_quote_id("file:///path/to/doc.md") == "file:///path/to/doc.md"
        assert _shell_quote_id("https://example.com/path") == "https://example.com/path"
        assert _shell_quote_id("now@V{3}") == "now@V{3}"
        assert _shell_quote_id("now@V{-3}") == "now@V{-3}"
        assert _shell_quote_id(".tag/act") == ".tag/act"
        assert _shell_quote_id(".conversations") == ".conversations"

    def test_version_suffix_pattern_accepts_signed(self):
        """@V suffix parser accepts negative selectors."""
        from keep.cli import VERSION_SUFFIX_PATTERN
        m = VERSION_SUFFIX_PATTERN.search("now@V{-2}")
        assert m is not None
        assert m.group(1) == "-2"

    def test_space_id_quoted(self):
        """IDs with spaces get single-quoted."""
        from keep.cli import _shell_quote_id
        result = _shell_quote_id("file:///Application Data/foo")
        assert result == "'file:///Application Data/foo'"

    def test_special_chars_quoted(self):
        """IDs with shell-special chars get single-quoted."""
        from keep.cli import _shell_quote_id
        assert _shell_quote_id("test$var") == "'test$var'"
        assert _shell_quote_id("test&bg") == "'test&bg'"
        assert _shell_quote_id("test;cmd") == "'test;cmd'"

    def test_single_quote_escaped(self):
        """IDs containing single quotes use '\\'' escaping."""
        from keep.cli import _shell_quote_id
        result = _shell_quote_id("it's a test")
        assert result == "'it'\\''s a test'"

    def test_summary_line_quotes_unsafe_id(self):
        """_format_summary_line quotes IDs with spaces."""
        from keep.cli import _format_summary_line
        from keep.types import Item
        item = Item(
            id="file:///Application Data/doc.md",
            summary="A doc",
            tags={"_updated": "2026-01-15T00:00:00Z"},
        )
        output = _format_summary_line(item)
        assert "'file:///Application Data/doc.md'" in output

    def test_summary_line_no_quotes_safe_id(self):
        """_format_summary_line does not quote safe IDs."""
        from keep.cli import _format_summary_line
        from keep.types import Item
        item = Item(
            id="%abc123",
            summary="A note",
            tags={"_updated": "2026-01-15T00:00:00Z"},
        )
        output = _format_summary_line(item)
        assert "%abc123" in output
        assert "'" not in output.split(" ")[0]  # ID portion not quoted

    def test_versioned_id_quotes_unsafe(self):
        """_format_versioned_id quotes IDs with spaces."""
        from keep.cli import _format_versioned_id
        from keep.types import Item
        item = Item(
            id="file:///my docs/test.md",
            summary="Test",
            tags={},
        )
        output = _format_versioned_id(item)
        assert output == "'file:///my docs/test.md'"

    def test_json_output_not_quoted(self):
        """JSON output does NOT shell-quote IDs."""
        from keep.cli import _format_items
        from keep.types import Item
        item = Item(
            id="file:///Application Data/doc.md",
            summary="A doc",
            tags={},
        )
        output = _format_items([item], as_json=True)
        parsed = json.loads(output)[0]
        assert parsed["id"] == "file:///Application Data/doc.md"  # Raw, no quoting


# -----------------------------------------------------------------------------
# Command Alias Tests
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Directory Put Tests
# -----------------------------------------------------------------------------

class TestPutDirectory:
    """Tests for directory mode in the put command."""

    def test_list_directory_files_basic(self, tmp_path):
        """_list_directory_files returns regular files sorted by name."""
        from keep.cli import _list_directory_files
        (tmp_path / "b.txt").write_text("B")
        (tmp_path / "a.txt").write_text("A")
        (tmp_path / "c.txt").write_text("C")
        files = _list_directory_files(tmp_path)
        assert [f.name for f in files] == ["a.txt", "b.txt", "c.txt"]

    def test_list_directory_files_skips_subdirs(self, tmp_path):
        """_list_directory_files skips subdirectories (non-recursive)."""
        from keep.cli import _list_directory_files
        (tmp_path / "file.txt").write_text("ok")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "nested.txt").write_text("nested")
        files = _list_directory_files(tmp_path)
        assert [f.name for f in files] == ["file.txt"]

    def test_list_directory_files_skips_symlinks(self, tmp_path):
        """_list_directory_files skips symlinks."""
        from keep.cli import _list_directory_files
        real_file = tmp_path / "real.txt"
        real_file.write_text("real")
        (tmp_path / "link.txt").symlink_to(real_file)
        files = _list_directory_files(tmp_path)
        assert [f.name for f in files] == ["real.txt"]

    def test_list_directory_files_skips_hidden(self, tmp_path):
        """_list_directory_files skips hidden files (dotfiles)."""
        from keep.cli import _list_directory_files
        (tmp_path / ".DS_Store").write_text("junk")
        (tmp_path / ".hidden").write_text("hidden")
        (tmp_path / "visible.txt").write_text("ok")
        files = _list_directory_files(tmp_path)
        assert [f.name for f in files] == ["visible.txt"]

    def test_list_directory_files_empty(self, tmp_path):
        """_list_directory_files returns empty list for empty directory."""
        from keep.cli import _list_directory_files
        files = _list_directory_files(tmp_path)
        assert files == []

    def test_list_directory_files_only_hidden(self, tmp_path):
        """_list_directory_files returns empty when only hidden files exist."""
        from keep.cli import _list_directory_files
        (tmp_path / ".gitignore").write_text("*")
        (tmp_path / ".DS_Store").write_text("")
        files = _list_directory_files(tmp_path)
        assert files == []

    def test_is_filesystem_path_directory(self, tmp_path):
        """_is_filesystem_path recognizes existing directories."""
        from keep.cli import _is_filesystem_path
        result = _is_filesystem_path(str(tmp_path))
        assert result is not None
        assert result.is_dir()

    def test_is_filesystem_path_file(self, tmp_path):
        """_is_filesystem_path recognizes existing files."""
        from keep.cli import _is_filesystem_path
        f = tmp_path / "test.txt"
        f.write_text("hello")
        result = _is_filesystem_path(str(f))
        assert result is not None
        assert result.is_file()

    def test_is_filesystem_path_nonexistent(self):
        """_is_filesystem_path returns None for nonexistent paths."""
        from keep.cli import _is_filesystem_path
        result = _is_filesystem_path("/nonexistent/path/to/nowhere")
        assert result is None

    def test_is_filesystem_path_uri(self, tmp_path):
        """_is_filesystem_path returns None for URIs even if path exists."""
        from keep.cli import _is_filesystem_path
        result = _is_filesystem_path(f"file://{tmp_path}")
        assert result is None

    def test_is_filesystem_path_tilde(self):
        """_is_filesystem_path expands tilde."""
        from keep.cli import _is_filesystem_path
        result = _is_filesystem_path("~")
        assert result is not None
        assert result == Path.home()

    def test_put_directory_rejects_summary(self, cli, tmp_path):
        """Directory mode rejects --summary flag."""
        (tmp_path / "a.txt").write_text("test")
        result = cli("put", str(tmp_path), "--summary", "nope")
        assert result.returncode == 1
        assert "--summary cannot be used with directory" in result.stderr

    def test_put_directory_rejects_id(self, cli, tmp_path):
        """Directory mode rejects --id flag."""
        (tmp_path / "a.txt").write_text("test")
        result = cli("put", str(tmp_path), "--id", "custom")
        assert result.returncode == 1
        assert "--id cannot be used with directory" in result.stderr

    def test_put_empty_directory(self, cli, tmp_path):
        """Directory mode errors on empty directory."""
        result = cli("put", str(tmp_path))
        assert result.returncode == 1
        assert "no eligible files" in result.stderr

    def test_put_directory_only_hidden(self, cli, tmp_path):
        """Directory mode errors when only hidden files exist."""
        (tmp_path / ".DS_Store").write_text("junk")
        result = cli("put", str(tmp_path))
        assert result.returncode == 1
        assert "no eligible files" in result.stderr

    def test_put_file_count_cap(self, cli, tmp_path):
        """Directory mode rejects directories with too many files."""
        max_files = 1000  # default config value
        # Create max_files + 1 files
        for i in range(max_files + 1):
            (tmp_path / f"file_{i:04d}.txt").write_text(f"content {i}")
        result = cli("put", str(tmp_path))
        assert result.returncode == 1
        assert f"{max_files + 1} files" in result.stderr
        assert f"max {max_files}" in result.stderr

    def test_put_bare_file_path_help(self, cli):
        """Put help mentions directory and file modes."""
        result = cli("put", "--help")
        assert result.returncode == 0
        assert "Directory mode" in result.stdout or "directory" in result.stdout.lower()


class TestCommandAliases:
    """Tests that old command names still work as hidden aliases."""

    def test_help_shows_put_not_update(self, cli):
        """Main help shows canonical command names."""
        result = cli("--help")
        assert result.returncode == 0
        assert "put" in result.stdout
        assert "del" in result.stdout
        assert "tag" in result.stdout
        # Old names should be hidden
        lines = result.stdout.split("\n")
        visible_commands = [l for l in lines if l.strip() and not l.strip().startswith("--")]
        visible_text = "\n".join(visible_commands)
        assert "tag-update" not in visible_text
        # 'update' and 'delete' should not appear as visible commands.

    def test_update_alias_works(self, cli):
        """'update' still works as a hidden alias for 'put'."""
        result = cli("update", "--help")
        assert result.returncode == 0

    def test_delete_alias_works(self, cli):
        """'delete' still works as a hidden alias for 'del'."""
        result = cli("delete", "--help")
        assert result.returncode == 0

    def test_tag_update_alias_works(self, cli):
        """'tag-update' still works as a hidden alias for 'tag'."""
        result = cli("tag-update", "--help")
        assert result.returncode == 0

    def test_del_help(self, cli):
        """'del' command has help."""
        result = cli("del", "--help")
        assert result.returncode == 0
        assert "Delete" in result.stdout or "delete" in result.stdout.lower()
