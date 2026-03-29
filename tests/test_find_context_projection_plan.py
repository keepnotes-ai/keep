# Copyright (c) 2026 Inguz Outcomes LLC.  All rights reserved.
"""Tests for the internal find-context projection plan."""

from unittest.mock import MagicMock

from keep.document_store import VersionInfo
from keep.projections import plan_find_context_render
from keep.types import Item, PartRef


def _item(id="test", summary="Test summary", score=0.9, tags=None):
    return Item(id=id, summary=summary, score=score, tags=tags or {"_updated_date": "2026-02-20"})


class TestFindContextProjectionPlan:
    """Plan-level regression coverage for find-context projection behavior."""

    def test_budget_trace_records_forced_summary_overspend(self):
        items = [
            _item(id="first", summary="A" * 200),
            _item(id="second", summary="B" * 200),
        ]

        plan = plan_find_context_render(items, token_budget=30)

        assert len(plan.blocks) == 1
        assert plan.tokens_remaining < 0
        decision = plan.budget_trace[0]
        assert decision.kind == "summary"
        assert decision.phase == "summary"
        assert decision.policy == "force"
        assert decision.accepted is True
        assert decision.allocated_tokens > 30

    def test_deep_sections_precede_detail_sections(self):
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = [
            PartRef(part_num=0, summary="Overview of the topic"),
            PartRef(part_num=1, summary="Details and analysis"),
        ]
        keeper.list_versions.return_value = []
        keeper.list_versions_around.return_value = []

        items = [_item(id=f"doc-{i}", summary=f"Doc {i}") for i in range(3)]
        deep_groups = {
            "doc-0": [_item(id="deep-x", summary="Deep X", score=0.9)],
        }
        results = FindResults(items, deep_groups=deep_groups)

        plan = plan_find_context_render(results, keeper=keeper, token_budget=5000, deep_primary_cap=3)

        first_block = plan.blocks[0]
        section_kinds = [section.kind for section in first_block.sections]
        assert section_kinds[:2] == ["summary", "deep-anchor"]
        assert "story" in section_kinds
        assert section_kinds.index("story") < section_kinds.index("parts")

    def test_compact_mode_skips_thread_and_story_sections(self):
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = [
            PartRef(part_num=0, summary="Overview"),
            PartRef(part_num=1, summary="Details"),
        ]
        keeper.list_versions_around.return_value = [
            VersionInfo(version=4, summary="Before", tags={}, created_at="2026-01-01", content_hash="a"),
            VersionInfo(version=5, summary="Matched", tags={}, created_at="2026-01-02", content_hash="b"),
        ]
        keeper.list_versions.return_value = []

        items = [_item(id="Joanna", summary="Entity", tags={"_entity": "true"})]
        deep_groups = {
            "Joanna": [_item(
                id="conv3-session11",
                summary="Head",
                score=0.9,
                tags={"_focus_summary": "Matched evidence", "_focus_version": "5", "_focus_part": "1"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)

        plan = plan_find_context_render(results, keeper=keeper, token_budget=200, deep_primary_cap=3)

        assert plan.compact_mode is True
        section_kinds = [section.kind for block in plan.blocks for section in block.sections]
        assert "thread" not in section_kinds
        assert "story" not in section_kinds

    def test_primary_cap_is_applied_before_summary_budget_spend(self):
        from keep.api import FindResults

        items = [
            _item(id="top-0", summary="X" * 280, score=0.99),
            _item(id="top-1", summary="Y" * 280, score=0.98),
            _item(id="grp-a", summary="Group A", score=0.70),
            _item(id="grp-b", summary="Group B", score=0.69),
            _item(
                id="Joanna",
                summary="Entity",
                score=0.50,
                tags={"_updated_date": "2026-02-20", "_entity": "true"},
            ),
        ]
        deep_groups = {
            "grp-a": [_item(id="a-1", summary="A deep", score=0.8)],
            "grp-b": [_item(id="b-1", summary="B deep", score=0.7)],
            "Joanna": [_item(id="j-1", summary="Joanna deep", score=0.9)],
        }
        results = FindResults(items, deep_groups=deep_groups)

        plan = plan_find_context_render(results, token_budget=120, deep_primary_cap=3)

        block_ids = [block.item.id for block in plan.blocks]
        assert "Joanna" in block_ids
        assert "top-0" not in block_ids

    def test_budget_trace_records_partial_legacy_detail_section(self):
        keeper = MagicMock()
        keeper.list_parts.return_value = [
            PartRef(part_num=1, summary="X" * 80),
            PartRef(part_num=2, summary="Y" * 80),
        ]
        keeper.list_versions.return_value = []
        keeper.list_versions_around.return_value = []

        items = [
            _item(id="a", summary="One"),
            _item(id="b", summary="Two"),
        ]

        plan = plan_find_context_render(items, keeper=keeper, token_budget=50)

        parts_section = next(section for section in plan.blocks[0].sections if section.kind == "parts")
        assert parts_section.policy == "fit-section-legacy-header"
        assert parts_section.skipped_lines == 1
        assert parts_section.allocated_tokens < parts_section.requested_tokens

        parts_decision = next(decision for decision in plan.budget_trace if decision.kind == "parts")
        assert parts_decision.phase == "detail"
        assert parts_decision.policy == "fit-section-legacy-header"
        assert parts_decision.skipped_lines == 1
        assert parts_decision.accepted is True
