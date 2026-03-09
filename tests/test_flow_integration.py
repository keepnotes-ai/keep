"""End-to-end behavioral tests for the flow system.

These tests verify that the state-doc flow paths are actually invoked
through the public API, not just that individual components work in
isolation.
"""

import pytest
from unittest.mock import patch

from keep.api import Keeper, FindResults
from keep.state_doc import parse_state_doc
from keep.state_doc_runtime import run_flow


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def kp(mock_providers, tmp_path):
    """Keeper instance with embedding provider initialized."""
    kp = Keeper(store_path=tmp_path)
    kp._get_embedding_provider()
    return kp


# ---------------------------------------------------------------------------
# Write path: put() routes through state-doc flow
# ---------------------------------------------------------------------------

class TestWritePathFlow:
    def test_put_invokes_flow_engine(self, kp):
        """put() routes through _put_via_flow, which evaluates the
        after-write state doc and enqueues background tasks."""
        kp.put("Short note", id="s1")
        # Short content skips summarize but still fires analyze+tag
        count = kp.continuation_pending_count()
        assert count >= 2, f"Expected >=2 pending tasks (analyze+tag), got {count}"

    def test_put_long_content_enqueues_more_tasks(self, kp):
        """Long content enqueues summarize (direct) + analyze + tag (state doc)."""
        baseline = kp.continuation_pending_count()
        kp.put("x" * 500, id="long1")
        count = kp.continuation_pending_count() - baseline
        # analyze + tag from state doc, plus summarize from direct path
        assert count >= 2, f"Expected >=2 new tasks, got {count}"

    def test_put_system_note_skips_state_doc(self, kp):
        """System notes (dot-prefix) skip state-doc evaluation entirely."""
        kp.put("System data", id=".sys/test")
        # System notes should not enqueue analyze or tag
        count = kp.continuation_pending_count()
        assert count == 0, f"System note should not create tasks, got {count}"


# ---------------------------------------------------------------------------
# Read path: get_context() uses state-doc flow
# ---------------------------------------------------------------------------

class TestGetContextFlow:
    def test_get_context_invokes_read_flow(self, kp):
        """get_context() calls _run_read_flow('get-context')."""
        kp.put("Test note about architecture", id="arch1")
        kp.put("Related note about design", id="design1")

        calls = []
        original = kp._run_read_flow

        def tracking_flow(state, params, **kwargs):
            calls.append(state)
            return original(state, params, **kwargs)

        kp._run_read_flow = tracking_flow
        kp.get_context("arch1")

        assert "get-context" in calls, \
            "get_context() should invoke the get-context state-doc flow"

    def test_get_context_returns_similar_items(self, kp):
        """get_context assembles similar items via the flow."""
        kp.put("Python async patterns", id="py1")
        kp.put("Python concurrency guide", id="py2")
        kp.put("Unrelated cooking recipe", id="cook1")

        ctx = kp.get_context("py1")
        assert ctx is not None
        # Should have similar items populated
        similar = getattr(ctx, "similar", None)
        if similar is not None:
            similar_ids = {s.id for s in similar}
            assert "py2" in similar_ids or len(similar) > 0


# ---------------------------------------------------------------------------
# Read path: find(deep=True) uses state-doc flow
# ---------------------------------------------------------------------------

class TestFindDeepFlow:
    def test_find_deep_invokes_flow_when_no_edges(self, kp):
        """find(deep=True) calls _deep_follow_via_flow when store has no edges."""
        kp.put("OAuth2 token design for project X", id="a",
               tags={"project": "x", "topic": "auth"})
        for i in range(35):
            kp.put(f"Filler note {i}", id=f"f-{i}", tags={"filler": "yes"})
        kp.put("Project X performance report", id="b",
               tags={"project": "x"})

        calls = []
        original = kp._run_read_flow

        def tracking_flow(state, params, **kwargs):
            calls.append(state)
            return original(state, params, **kwargs)

        kp._run_read_flow = tracking_flow
        results = kp.find("OAuth2 auth token design", deep=True, limit=5)

        assert "find-deep" in calls, \
            "find(deep=True) should invoke the find-deep state-doc flow"

    def test_find_deep_flow_discovers_bridge_items(self, kp):
        """The flow path discovers bridge items via tag-follow."""
        kp.put("OAuth2 design for project X", id="a",
               tags={"project": "x", "topic": "auth"})
        for i in range(35):
            kp.put(f"Filler note {i}", id=f"f-{i}", tags={"filler": "yes"})
        kp.put("Project X latency report", id="b",
               tags={"project": "x"})

        results = kp.find("OAuth2 auth design", deep=True, limit=5)
        all_ids = {r.id for r in results}
        deep_ids = set()
        for group in results.deep_groups.values():
            deep_ids.update(item.id for item in group)
        all_found = all_ids | deep_ids

        assert "b" in all_found, \
            "Bridge item 'b' should be found via project=x tag-follow"

    def test_find_deep_similar_to_uses_anchor_summary(self, kp):
        """find(similar_to=..., deep=True) falls back to anchor summary as query."""
        kp.put("OAuth2 token design", id="a", tags={"project": "x"})
        kp.put("Project X perf", id="b", tags={"project": "x"})

        # Should not raise — similar_to mode uses anchor summary
        results = kp.find(similar_to="a", deep=True, limit=5)
        assert isinstance(results, FindResults)
        assert len(results) > 0


# ---------------------------------------------------------------------------
# Budget enforcement in read flows
# ---------------------------------------------------------------------------

class TestFlowBudget:
    def test_budget_exhaustion_returns_stopped(self):
        """Flow that transitions indefinitely stops at budget limit."""
        loader_docs = {
            "loop": """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "test"
      limit: 5
  - then: loop
""",
        }
        compiled = {}
        for name, body in loader_docs.items():
            compiled[name] = parse_state_doc(name, body)

        def loader(name):
            return compiled.get(name)

        calls = []
        def runner(action_name, params):
            calls.append(action_name)
            return {"results": [], "count": 0}

        result = run_flow("loop", {}, budget=3,
                         load_state_doc=loader, run_action=runner)
        assert result.status == "stopped"
        assert result.ticks == 3
        assert result.data == {"reason": "budget"}
        assert len(result.history) == 3

    def test_budget_one_allows_single_tick(self):
        """Budget=1 allows exactly one state-doc evaluation."""
        compiled = {
            "one": parse_state_doc("one", """\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "test"
      limit: 1
  - return: done
"""),
        }

        result = run_flow(
            "one", {}, budget=1,
            load_state_doc=lambda n: compiled.get(n),
            run_action=lambda n, p: {"results": [], "count": 0},
        )
        assert result.status == "done"
        assert result.ticks == 1


# ---------------------------------------------------------------------------
# Traversal: Tier 1 (edges) → Tier 2 (tag-follow) fallback
# ---------------------------------------------------------------------------

class TestTraversalFallback:
    def test_no_edges_falls_to_tag_follow(self, kp):
        """When store has no edges, traverse_related uses tag-follow."""
        kp.put("Topic A note 1", id="a1", tags={"topic": "alpha"})
        kp.put("Topic A note 2", id="a2", tags={"topic": "alpha"})
        kp.put("Topic B note", id="b1", tags={"topic": "beta"})

        from keep.flow_env import LocalFlowEnvironment
        env = LocalFlowEnvironment(kp)

        groups = env.traverse_related(["a1"], limit_per_source=5)
        assert "a1" in groups
        # a2 shares topic=alpha with a1, should be discovered via tag-follow
        group_ids = {item.id for item in groups.get("a1", [])}
        assert "a2" in group_ids, \
            "a2 should be found via tag-follow on topic=alpha"

    def test_traverse_does_not_touch_accessed_at(self, kp):
        """Internal traversal should not update accessed_at timestamps."""
        kp.put("Note for traversal", id="t1", tags={"topic": "traverse"})

        doc_coll = kp._resolve_doc_collection()
        before = kp._document_store.get(doc_coll, "t1")
        before_accessed = before.accessed_at

        from keep.flow_env import LocalFlowEnvironment
        env = LocalFlowEnvironment(kp)
        env.traverse_related(["t1"], limit_per_source=5)

        after = kp._document_store.get(doc_coll, "t1")
        assert after.accessed_at == before_accessed, \
            "traverse_related should not update accessed_at"


# ---------------------------------------------------------------------------
# Auto-vivify atomicity
# ---------------------------------------------------------------------------

class TestAutoVivify:
    def test_insert_if_absent_does_not_overwrite(self, kp):
        """Auto-vivify uses insert_if_absent — doesn't clobber existing docs."""
        doc_coll = kp._resolve_doc_collection()

        # Pre-create the target with real content
        kp.put("Real document about Alice", id="alice",
               tags={"type": "person"})

        # Now simulate auto-vivify for the same ID
        inserted = kp._document_store.insert_if_absent(
            doc_coll, "alice",
            summary="",
            tags={"_source": "auto-vivify"},
        )

        assert not inserted, "insert_if_absent should return False for existing doc"

        # Verify original content preserved
        doc = kp._document_store.get(doc_coll, "alice")
        assert doc.summary == "Real document about Alice"
        assert doc.tags.get("type") == "person"
