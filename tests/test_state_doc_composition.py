"""Tests for state doc fragment composition."""

import pytest

from keep.api import Keeper
from keep.state_doc import (
    StateDoc,
    CompiledRule,
    StateDocFragment,
    parse_state_doc,
    parse_fragment,
    merge_fragments,
)


# ---------------------------------------------------------------------------
# Fragment parsing
# ---------------------------------------------------------------------------

class TestParseFragment:

    def test_basic_fragment(self):
        body = """\
rules:
  - when: "item.has_uri"
    id: my-step
    do: my_action
"""
        frag = parse_fragment("my-frag", body)
        assert frag.name == "my-frag"
        assert frag.order == "after"
        assert len(frag.rules) == 1
        assert frag.rules[0].id == "my-step"
        assert frag.rules[0].do == "my_action"

    def test_order_before(self):
        body = """\
order: before
rules:
  - id: pre-step
    do: pre_action
"""
        frag = parse_fragment("pre", body)
        assert frag.order == "before"

    def test_order_positional(self):
        body = """\
order: after:summary
rules:
  - id: post-summary
    do: enrich
"""
        frag = parse_fragment("enricher", body)
        assert frag.order == "after:summary"

    def test_missing_rules_raises(self):
        with pytest.raises(ValueError, match="rules must be a list"):
            parse_fragment("bad", "order: after\n")

    def test_non_mapping_raises(self):
        with pytest.raises(ValueError, match="must be a YAML mapping"):
            parse_fragment("bad", "just a string")

    def test_default_order(self):
        body = "rules:\n  - id: x\n    do: y\n"
        frag = parse_fragment("f", body)
        assert frag.order == "after"


# ---------------------------------------------------------------------------
# Merge fragments
# ---------------------------------------------------------------------------

def _make_rule(id: str, do: str = "noop") -> CompiledRule:
    return CompiledRule(id=id, do=do)


def _make_base(rule_ids: list[str], match: str = "all") -> StateDoc:
    return StateDoc(
        name="test",
        match=match,
        rules=[_make_rule(rid) for rid in rule_ids],
        post=[],
    )


def _make_frag(name: str, rule_ids: list[str], order: str = "after") -> StateDocFragment:
    return StateDocFragment(
        name=name,
        order=order,
        rules=[_make_rule(rid) for rid in rule_ids],
    )


class TestMergeFragments:

    def test_empty_fragments(self):
        base = _make_base(["a", "b"])
        result = merge_fragments(base, [])
        assert [r.id for r in result.rules] == ["a", "b"]

    def test_append_after(self):
        base = _make_base(["a", "b"])
        frag = _make_frag("f1", ["c", "d"], order="after")
        result = merge_fragments(base, [frag])
        assert [r.id for r in result.rules] == ["a", "b", "c", "d"]

    def test_prepend_before(self):
        base = _make_base(["a", "b"])
        frag = _make_frag("f1", ["x"], order="before")
        result = merge_fragments(base, [frag])
        assert [r.id for r in result.rules] == ["x", "a", "b"]

    def test_positional_after_rule(self):
        base = _make_base(["a", "b", "c"])
        frag = _make_frag("f1", ["x"], order="after:a")
        result = merge_fragments(base, [frag])
        assert [r.id for r in result.rules] == ["a", "x", "b", "c"]

    def test_positional_before_rule(self):
        base = _make_base(["a", "b", "c"])
        frag = _make_frag("f1", ["x"], order="before:c")
        result = merge_fragments(base, [frag])
        assert [r.id for r in result.rules] == ["a", "b", "x", "c"]

    def test_positional_target_not_found_falls_back(self):
        base = _make_base(["a", "b"])
        frag = _make_frag("f1", ["x"], order="after:nonexistent")
        result = merge_fragments(base, [frag])
        # Falls back to append
        assert [r.id for r in result.rules] == ["a", "b", "x"]

    def test_multiple_fragments_ordering(self):
        base = _make_base(["a", "b"])
        frags = [
            _make_frag("f1", ["x"], order="before"),
            _make_frag("f2", ["y"], order="after"),
            _make_frag("f3", ["z"], order="after:a"),
        ]
        result = merge_fragments(base, frags)
        assert [r.id for r in result.rules] == ["x", "a", "z", "b", "y"]

    def test_preserves_match_mode(self):
        base = _make_base(["a"], match="sequence")
        frag = _make_frag("f1", ["x"])
        result = merge_fragments(base, [frag])
        assert result.match == "sequence"

    def test_preserves_post(self):
        base = StateDoc(
            name="test", match="all",
            rules=[_make_rule("a")],
            post=[_make_rule("done")],
        )
        frag = _make_frag("f1", ["x"])
        result = merge_fragments(base, [frag])
        assert len(result.post) == 1
        assert result.post[0].id == "done"

    def test_multiple_rules_positional_insert(self):
        base = _make_base(["a", "b", "c"])
        frag = _make_frag("f1", ["x", "y"], order="after:a")
        result = merge_fragments(base, [frag])
        assert [r.id for r in result.rules] == ["a", "x", "y", "b", "c"]


# ---------------------------------------------------------------------------
# Integration: parse + merge
# ---------------------------------------------------------------------------

class TestParseAndMerge:

    def test_parse_base_and_fragment_then_merge(self):
        base_body = """\
match: all
rules:
  - id: summary
    when: "item.content_length > 500"
    do: summarize
  - id: tagged
    do: tag
post:
  - return: done
"""
        frag_body = """\
order: after:summary
rules:
  - id: custom-enrich
    when: "item.has_uri"
    do: enrich
"""
        base = parse_state_doc("after-write", base_body)
        frag = parse_fragment("enricher", frag_body)
        merged = merge_fragments(base, [frag])

        assert [r.id for r in merged.rules] == ["summary", "custom-enrich", "tagged"]
        assert merged.match == "all"
        assert len(merged.post) == 1


# ---------------------------------------------------------------------------
# Loader integration: _load_state_doc with real Keeper
# ---------------------------------------------------------------------------

class TestLoaderIntegration:

    def test_fragment_merged_into_builtin(self, mock_providers, tmp_path):
        """A child fragment at .state/after-write/X gets merged into the builtin."""
        kp = Keeper(store_path=tmp_path)
        frag_body = """\
order: after:summary
rules:
  - id: custom-step
    when: "item.has_uri"
    do: custom_action
"""
        kp.put(frag_body, id=".state/after-write/my-extension")

        doc = kp._load_state_doc("after-write")
        rule_ids = [r.id for r in doc.rules]
        assert "custom-step" in rule_ids
        assert "summary" in rule_ids
        # custom-step should be right after summary
        si = rule_ids.index("summary")
        ci = rule_ids.index("custom-step")
        assert ci == si + 1

    def test_inactive_fragment_skipped(self, mock_providers, tmp_path):
        """Fragments tagged active=false are not merged."""
        kp = Keeper(store_path=tmp_path)
        frag_body = "rules:\n  - id: skipped\n    do: noop\n"
        kp.put(frag_body, id=".state/after-write/disabled-step")
        kp.tag(".state/after-write/disabled-step", tags={"active": "false"})

        doc = kp._load_state_doc("after-write")
        rule_ids = [r.id for r in doc.rules]
        assert "skipped" not in rule_ids

    def test_multiple_fragments_sorted_by_id(self, mock_providers, tmp_path):
        """Fragments are processed in alphabetical order by id."""
        kp = Keeper(store_path=tmp_path)
        kp.put("rules:\n  - id: z-step\n    do: z\n", id=".state/after-write/zzz")
        kp.put("rules:\n  - id: a-step\n    do: a\n", id=".state/after-write/aaa")

        doc = kp._load_state_doc("after-write")
        rule_ids = [r.id for r in doc.rules]
        # Both appended (default order: after), aaa first
        ai = rule_ids.index("a-step")
        zi = rule_ids.index("z-step")
        assert ai < zi

    def test_no_fragments_returns_base(self, mock_providers, tmp_path):
        """Without fragments, returns the base doc unchanged."""
        kp = Keeper(store_path=tmp_path)
        doc = kp._load_state_doc("after-write")
        # Should have the builtin rules
        rule_ids = [r.id for r in doc.rules]
        assert "summary" in rule_ids
        assert "analyzed" in rule_ids
