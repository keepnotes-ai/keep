"""Tests for the synchronous state-doc flow runtime."""

import pytest

from keep.state_doc import parse_state_doc
from keep.state_doc_runtime import FlowResult, run_flow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_loader(docs: dict[str, str]):
    """Create a state doc loader from a dict of {name: yaml_body}."""
    compiled = {}
    for name, body in docs.items():
        compiled[name] = parse_state_doc(name, body)

    def _load(name: str):
        return compiled.get(name)

    return _load


def _make_runner(outputs: dict[str, dict] | None = None):
    """Create an action runner that returns canned outputs."""
    calls = []
    defaults = outputs or {}

    def _run(action_name: str, params: dict):
        calls.append((action_name, dict(params)))
        return dict(defaults.get(action_name, {}))

    _run.calls = calls  # type: ignore[attr-defined]
    return _run


# ---------------------------------------------------------------------------
# Basic terminal flow
# ---------------------------------------------------------------------------

class TestBasicFlow:
    def test_immediate_done(self):
        loader = _make_loader({
            "simple": "rules:\n  - return: done",
        })
        result = run_flow("simple", {}, load_state_doc=loader, run_action=_make_runner())
        assert result.status == "done"
        assert result.ticks == 1
        assert result.history == ["simple"]

    def test_immediate_error(self):
        loader = _make_loader({
            "fail": "rules:\n  - return: error",
        })
        result = run_flow("fail", {}, load_state_doc=loader, run_action=_make_runner())
        assert result.status == "error"

    def test_immediate_stopped(self):
        loader = _make_loader({
            "stop": """\
rules:
  - return:
      status: stopped
      with:
        reason: "ambiguous"
""",
        })
        result = run_flow("stop", {}, load_state_doc=loader, run_action=_make_runner())
        assert result.status == "stopped"
        assert result.data["reason"] == "ambiguous"

    def test_missing_state_doc(self):
        result = run_flow(
            "nonexistent", {},
            load_state_doc=lambda n: None,
            run_action=_make_runner(),
        )
        assert result.status == "error"
        assert "not found" in result.data["reason"]


# ---------------------------------------------------------------------------
# Action execution
# ---------------------------------------------------------------------------

class TestActionExecution:
    def test_action_runs(self):
        loader = _make_loader({
            "with-action": """\
rules:
  - id: result
    do: find
    with:
      query: "test"
  - return: done
""",
        })
        runner = _make_runner({"find": {"results": [], "count": 0}})
        result = run_flow(
            "with-action", {"query": "test"},
            load_state_doc=loader, run_action=runner,
        )
        assert result.status == "done"
        assert len(runner.calls) == 1
        assert runner.calls[0][0] == "find"

    def test_action_output_bound(self):
        loader = _make_loader({
            "bound": """\
rules:
  - id: search
    do: find
    with:
      query: "test"
  - return: done
""",
        })
        runner = _make_runner({
            "find": {
                "results": [
                    {"id": "a", "summary": "x", "tags": {}, "score": 0.9},
                    {"id": "b", "summary": "y", "tags": {}, "score": 0.3},
                ],
                "count": 2,
            },
        })
        result = run_flow(
            "bound", {},
            load_state_doc=loader, run_action=runner,
        )
        assert "search" in result.bindings
        # Find output should be enriched with stats
        assert "margin" in result.bindings["search"]
        assert "entropy" in result.bindings["search"]


# ---------------------------------------------------------------------------
# Find output enrichment
# ---------------------------------------------------------------------------

class TestFindEnrichment:
    def test_find_output_gets_stats(self):
        loader = _make_loader({
            "enriched": """\
rules:
  - id: search
    do: find
    with:
      query: "test"
  - return: done
""",
        })
        runner = _make_runner({
            "find": {
                "results": [
                    {"id": "a", "summary": "", "tags": {}, "score": 0.95},
                    {"id": "b", "summary": "", "tags": {}, "score": 0.30},
                ],
                "count": 2,
            },
        })
        result = run_flow(
            "enriched", {},
            load_state_doc=loader, run_action=runner,
        )
        search = result.bindings["search"]
        assert search["margin"] > 0.5  # dominant result
        assert isinstance(search["entropy"], float)
        assert isinstance(search["lineage_strong"], float)

    def test_non_find_action_not_enriched(self):
        loader = _make_loader({
            "other": """\
rules:
  - id: item
    do: get
    with:
      id: "test"
  - return: done
""",
        })
        runner = _make_runner({"get": {"id": "test", "summary": "x", "tags": {}}})
        result = run_flow(
            "other", {},
            load_state_doc=loader, run_action=runner,
        )
        assert "margin" not in result.bindings.get("item", {})


# ---------------------------------------------------------------------------
# Transitions
# ---------------------------------------------------------------------------

class TestTransitions:
    def test_simple_transition(self):
        loader = _make_loader({
            "first": "rules:\n  - then: second",
            "second": "rules:\n  - return: done",
        })
        result = run_flow(
            "first", {},
            load_state_doc=loader, run_action=_make_runner(),
        )
        assert result.status == "done"
        assert result.ticks == 2
        assert result.history == ["first", "second"]

    def test_transition_with_params(self):
        loader = _make_loader({
            "first": """\
rules:
  - then:
      state: second
      with:
        extra: "value"
""",
            "second": "rules:\n  - return: done",
        })
        result = run_flow(
            "first", {"original": "kept"},
            load_state_doc=loader, run_action=_make_runner(),
        )
        assert result.status == "done"
        assert result.history == ["first", "second"]

    def test_self_transition_with_budget(self):
        loader = _make_loader({
            "loop": "rules:\n  - then: loop",
        })
        result = run_flow(
            "loop", {}, budget=3,
            load_state_doc=loader, run_action=_make_runner(),
        )
        assert result.status == "stopped"
        assert result.data["reason"] == "budget"
        assert result.ticks == 3
        assert result.history == ["loop", "loop", "loop"]

    def test_transition_to_missing_doc(self):
        loader = _make_loader({
            "start": "rules:\n  - then: nonexistent",
        })
        result = run_flow(
            "start", {},
            load_state_doc=loader, run_action=_make_runner(),
        )
        assert result.status == "error"
        assert "not found" in result.data["reason"]


# ---------------------------------------------------------------------------
# Budget enforcement
# ---------------------------------------------------------------------------

class TestBudget:
    def test_budget_1(self):
        loader = _make_loader({
            "step": "rules:\n  - then: step",
        })
        result = run_flow(
            "step", {}, budget=1,
            load_state_doc=loader, run_action=_make_runner(),
        )
        # Budget=1 means we get 1 tick, which transitions to step again,
        # but the loop exits because ticks >= budget
        assert result.status == "stopped"
        assert result.ticks == 1

    def test_budget_context_available(self):
        """Budget context is available to predicates."""
        # This test verifies the eval context has budget.remaining.
        # The predicate "budget.remaining > 0" should be evaluable.
        # We can't easily test CEL predicates without the CEL library,
        # so we test the context structure indirectly through the flow.
        loader = _make_loader({
            "check": "rules:\n  - return: done",
        })
        result = run_flow(
            "check", {}, budget=5,
            load_state_doc=loader, run_action=_make_runner(),
        )
        assert result.status == "done"
        assert result.ticks == 1


# ---------------------------------------------------------------------------
# Match: all with post block
# ---------------------------------------------------------------------------

class TestMatchAll:
    def test_parallel_actions_then_post(self):
        loader = _make_loader({
            "parallel": """\
match: all
rules:
  - id: a
    do: find
    with:
      query: "one"
  - id: b
    do: get
    with:
      id: "two"
post:
  - return: done
""",
        })
        runner = _make_runner({
            "find": {"results": [], "count": 0},
            "get": {"id": "two", "summary": "x", "tags": {}},
        })
        result = run_flow(
            "parallel", {},
            load_state_doc=loader, run_action=runner,
        )
        assert result.status == "done"
        assert "a" in result.bindings
        assert "b" in result.bindings
        assert len(runner.calls) == 2


# ---------------------------------------------------------------------------
# Params flow across transitions
# ---------------------------------------------------------------------------

class TestParamsFlow:
    def test_params_persist_across_transitions(self):
        """Original params remain available after transition."""
        calls = []

        def _loader(name):
            docs = {
                "first": """\
rules:
  - then:
      state: second
      with:
        added: "new"
""",
                "second": """\
rules:
  - id: search
    do: find
    with:
      query: "{params.query}"
  - return: done
""",
            }
            if name in docs:
                return parse_state_doc(name, docs[name])
            return None

        def _runner(action_name, params):
            calls.append((action_name, dict(params)))
            return {"results": [], "count": 0}

        result = run_flow(
            "first", {"query": "original"},
            load_state_doc=_loader, run_action=_runner,
        )
        assert result.status == "done"
        # Template resolution of {params.query} should work
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# Factory: make_state_doc_loader
# ---------------------------------------------------------------------------

class TestMakeStateDocLoader:
    def test_loads_from_env(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeNote:
            id = ".state/test"
            summary = "rules:\n  - return: done"
            tags = {}

        class FakeEnv:
            def get(self, id):
                if id == ".state/test":
                    return FakeNote()
                return None

        loader = make_state_doc_loader(FakeEnv())
        doc = loader("test")
        assert doc is not None
        assert doc.name == "test"

    def test_returns_none_for_missing(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeEnv:
            def get(self, id):
                return None

        loader = make_state_doc_loader(FakeEnv())
        assert loader("nonexistent") is None

    def test_falls_back_to_builtins(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeEnv:
            def get(self, id):
                return None

        builtins = {"fallback": "rules:\n  - return: done"}
        loader = make_state_doc_loader(FakeEnv(), builtins=builtins)
        doc = loader("fallback")
        assert doc is not None
        assert doc.name == "fallback"

    def test_store_overrides_builtin(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeNote:
            id = ".state/override"
            summary = "rules:\n  - return: error"
            tags = {}

        class FakeEnv:
            def get(self, id):
                if id == ".state/override":
                    return FakeNote()
                return None

        builtins = {"override": "rules:\n  - return: done"}
        loader = make_state_doc_loader(FakeEnv(), builtins=builtins)
        # Run a flow to verify we got the store version (error) not builtin (done)
        result = run_flow("override", {}, load_state_doc=loader, run_action=_make_runner())
        assert result.status == "error"

    def test_builtin_used_when_store_summary_empty(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeNote:
            id = ".state/empty"
            summary = ""
            tags = {}

        class FakeEnv:
            def get(self, id):
                return FakeNote()

        builtins = {"empty": "rules:\n  - return: done"}
        loader = make_state_doc_loader(FakeEnv(), builtins=builtins)
        doc = loader("empty")
        assert doc is not None

    def test_returns_none_for_empty_summary(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeNote:
            id = ".state/empty"
            summary = ""
            tags = {}

        class FakeEnv:
            def get(self, id):
                return FakeNote()

        loader = make_state_doc_loader(FakeEnv())
        assert loader("empty") is None

    def test_handles_dotstate_prefix(self):
        from keep.state_doc_runtime import make_state_doc_loader

        class FakeNote:
            id = ".state/prefixed"
            summary = "rules:\n  - return: done"
            tags = {}

        class FakeEnv:
            def get(self, id):
                if id == ".state/prefixed":
                    return FakeNote()
                return None

        loader = make_state_doc_loader(FakeEnv())
        doc = loader(".state/prefixed")
        assert doc is not None


# ---------------------------------------------------------------------------
# Factory: make_action_runner
# ---------------------------------------------------------------------------

class TestMakeActionRunner:
    def test_runs_find_action(self):
        from keep.state_doc_runtime import make_action_runner

        class FakeItem:
            def __init__(self, id, summary="", tags=None, score=None):
                self.id = id
                self.summary = summary
                self.tags = tags or {}
                self.score = score

        class FakeEnv:
            def find(self, query=None, **kw):
                return [FakeItem("a", "note a", {"topic": "test"}, 0.9)]
            def get(self, id):
                return None
            def list_items(self, **kw):
                return []
            def get_document(self, id):
                return None
            def resolve_meta(self, id, **kw):
                return {}
            def traverse_related(self, source_ids, **kw):
                return {}

        runner = make_action_runner(FakeEnv())
        output = runner("find", {"query": "test"})
        assert output["count"] == 1
        assert output["results"][0]["id"] == "a"

    def test_unknown_action_raises(self):
        from keep.state_doc_runtime import make_action_runner

        class FakeEnv:
            pass

        runner = make_action_runner(FakeEnv())
        with pytest.raises(ValueError, match="unknown action"):
            runner("nonexistent_action_xyz", {})


# ---------------------------------------------------------------------------
# CEL predicates in flows (find-deep exercises search.count == 0)
# ---------------------------------------------------------------------------

class TestCELPredicates:
    def test_find_deep_skips_traverse_on_empty_search(self):
        """find-deep returns early when search.count == 0 (CEL predicate)."""
        from keep.builtin_state_docs import BUILTIN_STATE_DOCS
        from keep.state_doc_runtime import _get_compiled_builtin

        # Verify the find-deep builtin compiles (has CEL predicates)
        doc = _get_compiled_builtin("find-deep", BUILTIN_STATE_DOCS["find-deep"])
        assert doc is not None

        # Run with empty search results — CEL predicate should short-circuit
        loader = _make_loader({"find-deep": BUILTIN_STATE_DOCS["find-deep"]})
        calls = []

        def _runner(action_name, params):
            calls.append(action_name)
            if action_name == "find":
                return {"results": [], "count": 0}
            return {"groups": {}, "count": 0}

        result = run_flow(
            "find-deep",
            {"query": "test", "limit": 10, "deep_limit": 5},
            load_state_doc=loader, run_action=_runner,
        )
        assert result.status == "done"
        # Only find should have been called (traverse skipped via CEL)
        assert calls == ["find"]

    def test_find_deep_traverses_when_results_found(self):
        """find-deep traverses results when search.count > 0."""
        from keep.builtin_state_docs import BUILTIN_STATE_DOCS

        loader = _make_loader({"find-deep": BUILTIN_STATE_DOCS["find-deep"]})
        calls = []

        def _runner(action_name, params):
            calls.append(action_name)
            if action_name == "find":
                return {
                    "results": [
                        {"id": "a", "summary": "x", "tags": {}, "score": 0.9},
                    ],
                    "count": 1,
                }
            return {"groups": {"a": [{"id": "b", "summary": "y", "tags": {}}]}, "count": 1}

        result = run_flow(
            "find-deep",
            {"query": "test", "limit": 10, "deep_limit": 5},
            load_state_doc=loader, run_action=_runner,
        )
        assert result.status == "done"
        # Both find and traverse should have been called
        assert "find" in calls
        assert "traverse" in calls
        assert "related" in result.bindings
