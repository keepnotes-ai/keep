"""Tests for the extract_links action."""

from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock
from typing import Any

import pytest

from keep.actions.extract_links import (
    _parse_links,
    _resolve_internal_link,
    ExtractLinks,
)


# ---------------------------------------------------------------------------
# Link parsing
# ---------------------------------------------------------------------------

class TestParseLinks:

    def test_wiki_link(self):
        links = _parse_links("See [[My Note]] for details.")
        assert len(links) == 1
        assert links[0]["target"] == "My Note"
        assert links[0]["style"] == "wiki"

    def test_wiki_link_with_display(self):
        links = _parse_links("See [[target|display text]] here.")
        assert len(links) == 1
        assert links[0]["target"] == "target"

    def test_markdown_link(self):
        links = _parse_links("Read [the docs](./docs/README.md).")
        assert len(links) == 1
        assert links[0]["target"] == "./docs/README.md"
        assert links[0]["style"] == "markdown"

    def test_markdown_url(self):
        links = _parse_links("See [example](https://example.com).")
        assert len(links) == 1
        assert links[0]["target"] == "https://example.com"

    def test_image_not_captured(self):
        links = _parse_links("![alt](image.png)")
        assert len(links) == 0

    def test_mixed_styles(self):
        content = "Link to [[wiki-note]] and [md](./other.md) and [url](https://x.com)"
        links = _parse_links(content)
        assert len(links) == 3
        targets = {l["target"] for l in links}
        assert targets == {"wiki-note", "./other.md", "https://x.com"}

    def test_dedup(self):
        links = _parse_links("[[foo]] and [[foo]] again")
        assert len(links) == 1

    def test_skip_anchors(self):
        links = _parse_links("[section](#heading)")
        assert len(links) == 0

    def test_skip_mailto(self):
        links = _parse_links("[email](mailto:x@y.com)")
        assert len(links) == 0

    def test_empty_content(self):
        assert _parse_links("") == []
        assert _parse_links("No links here.") == []


# ---------------------------------------------------------------------------
# Link resolution
# ---------------------------------------------------------------------------

class TestResolveInternalLink:

    def _make_context(self, known_ids: set[str]):
        ctx = MagicMock()
        def _get(id):
            return MagicMock() if id in known_ids else None
        ctx.get = _get
        return ctx

    def test_direct_match(self):
        ctx = self._make_context({"my-note"})
        result = _resolve_internal_link("my-note", "source", ctx)
        assert result == "my-note"

    def test_file_uri_relative(self):
        ctx = self._make_context({"file:///vault/notes/other.md"})
        result = _resolve_internal_link(
            "other.md", "file:///vault/notes/current.md", ctx
        )
        assert result == "file:///vault/notes/other.md"

    def test_file_uri_without_extension(self):
        ctx = self._make_context({"file:///vault/notes/other.md"})
        result = _resolve_internal_link(
            "other", "file:///vault/notes/current.md", ctx
        )
        assert result == "file:///vault/notes/other.md"

    def test_file_uri_subdirectory(self):
        ctx = self._make_context({"file:///vault/notes/sub/deep.md"})
        result = _resolve_internal_link(
            "sub/deep.md", "file:///vault/notes/current.md", ctx
        )
        assert result == "file:///vault/notes/sub/deep.md"

    def test_not_found(self):
        ctx = self._make_context(set())
        result = _resolve_internal_link("missing", "file:///vault/x.md", ctx)
        assert result is None

    def test_bare_match_without_md(self):
        ctx = self._make_context({"CONTRIBUTING"})
        result = _resolve_internal_link("CONTRIBUTING.md", "source", ctx)
        assert result == "CONTRIBUTING"


# ---------------------------------------------------------------------------
# Action run
# ---------------------------------------------------------------------------

def _make_item(id: str, content: str, tags: dict | None = None):
    item = MagicMock()
    item.id = id
    item.summary = content[:100]
    item.content = content
    item.tags = tags or {}
    return item


def _make_context(items: dict[str, Any], item_id: str = "source.md"):
    ctx = MagicMock()
    ctx.item_id = item_id
    ctx.item_content = items.get(item_id, MagicMock()).content if item_id in items else ""

    def _get(id):
        return items.get(id)
    ctx.get = _get

    return ctx


class TestExtractLinksAction:

    def test_basic_wiki_link_resolved(self):
        source = _make_item("file:///vault/a.md", "See [[b]] for more.")
        target = _make_item("file:///vault/b.md", "Target content")
        ctx = _make_context(
            {"file:///vault/a.md": source, "file:///vault/b.md": target},
            item_id="file:///vault/a.md",
        )
        ctx.item_content = source.content

        result = ExtractLinks().run({"item_id": "file:///vault/a.md"}, ctx)

        assert not result.get("skipped")
        assert "file:///vault/b.md" in result["resolved"]
        # Should have set_tags mutation
        tag_mut = [m for m in result["mutations"] if m["op"] == "set_tags"]
        assert len(tag_mut) == 1
        assert "file:///vault/b.md" in tag_mut[0]["tags"]["references"]

    def test_external_url(self):
        source = _make_item("file:///vault/a.md", "See [docs](https://example.com).")
        ctx = _make_context(
            {"file:///vault/a.md": source},
            item_id="file:///vault/a.md",
        )
        ctx.item_content = source.content

        result = ExtractLinks().run({"item_id": "file:///vault/a.md"}, ctx)

        assert "https://example.com" in result["resolved"]
        # Should have put_item for auto-vivification + set_tags
        put_muts = [m for m in result["mutations"] if m["op"] == "put_item"]
        assert len(put_muts) == 1
        assert put_muts[0]["id"] == "https://example.com"

    def test_no_links_skipped(self):
        source = _make_item("a.md", "No links here.")
        ctx = _make_context({"a.md": source}, item_id="a.md")
        ctx.item_content = source.content
        result = ExtractLinks().run({"item_id": "a.md"}, ctx)
        assert result.get("skipped") is True

    def test_custom_tag_key(self):
        source = _make_item("a.md", "See [[b]].")
        target = _make_item("b", "Target")
        ctx = _make_context({"a.md": source, "b": target}, item_id="a.md")
        ctx.item_content = source.content

        result = ExtractLinks().run(
            {"item_id": "a.md", "tag": "links_to"}, ctx
        )

        tag_mut = [m for m in result["mutations"] if m["op"] == "set_tags"]
        assert "links_to" in tag_mut[0]["tags"]

    def test_create_targets_false(self):
        source = _make_item("a.md", "See [x](https://missing.com).")
        ctx = _make_context({"a.md": source}, item_id="a.md")
        ctx.item_content = source.content

        result = ExtractLinks().run(
            {"item_id": "a.md", "create_targets": "false"}, ctx
        )

        # URL still resolves (external URLs are always accepted)
        assert "https://missing.com" in result["resolved"]
        # But no put_item mutation
        put_muts = [m for m in result["mutations"] if m["op"] == "put_item"]
        assert len(put_muts) == 0

    def test_auto_vivify_internal(self):
        source = _make_item("file:///vault/a.md", "See [[missing-note]].")
        ctx = _make_context(
            {"file:///vault/a.md": source},
            item_id="file:///vault/a.md",
        )
        ctx.item_content = source.content

        result = ExtractLinks().run({"item_id": "file:///vault/a.md"}, ctx)

        assert not result.get("skipped")
        put_muts = [m for m in result["mutations"] if m["op"] == "put_item"]
        assert len(put_muts) == 1
        assert put_muts[0]["id"] == "file:///vault/missing-note.md"

    def test_merges_with_existing_references(self):
        source = _make_item(
            "a.md", "See [[new-link]].",
            tags={"references": ["existing-ref"]},
        )
        target = _make_item("new-link", "Target")
        ctx = _make_context(
            {"a.md": source, "new-link": target}, item_id="a.md",
        )
        ctx.item_content = source.content

        result = ExtractLinks().run({"item_id": "a.md"}, ctx)

        tag_mut = [m for m in result["mutations"] if m["op"] == "set_tags"]
        refs = tag_mut[0]["tags"]["references"]
        assert "existing-ref" in refs
        assert "new-link" in refs
