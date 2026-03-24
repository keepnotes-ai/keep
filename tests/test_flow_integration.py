"""End-to-end behavioral tests for the flow system.

These tests verify that the state-doc flow paths are actually invoked
through the public API, not just that individual components work in
isolation.
"""

import json

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
    """Verify put() dispatches background tasks via the after-write state doc.

    Task decisions are driven by evaluating the after-write state doc rules,
    NOT hardcoded in _put_direct.  See _dispatch_after_write_flow().
    """

    def test_put_enqueues_after_write_flow(self, kp):
        """put() enqueues a single after-write flow work item."""
        # Drain any migration-enqueued tasks first
        queue = kp._get_work_queue()
        queue.claim("drain", limit=200)
        kp.put("Short note", id="s1")
        claimed = queue.claim("test", limit=20)
        kinds = {t.kind for t in claimed}
        assert kinds == {"flow"}, f"Expected single flow item, got {kinds}"
        # Verify the flow is for after-write with the right item
        flow_item = claimed[0]
        assert flow_item.input.get("state") == "after-write"
        assert flow_item.input.get("item_id") == "s1"

    def test_put_system_note_skips_background(self, kp):
        """System notes: no background work enqueued at all."""
        # Drain any migration-enqueued tasks first
        queue = kp._get_work_queue()
        queue.claim("drain", limit=200)
        kp.put("System data", id=".sys/test")
        claimed = queue.claim("test", limit=20)
        assert len(claimed) == 0, f"Expected no work items for system note, got {len(claimed)}"


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

        assert "get" in calls, \
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
        assert result.data["reason"] == "budget"
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


# ---------------------------------------------------------------------------
# Cursor encoding/decoding and flow resumption
# ---------------------------------------------------------------------------

class TestFlowCursor:
    def test_cursor_round_trip(self):
        """encode_cursor / decode_cursor are symmetric."""
        from keep.state_doc_runtime import encode_cursor, decode_cursor

        token = encode_cursor("query-explore", 3, {"search": {"count": 5}})
        assert isinstance(token, str)
        assert len(token) > 0

        decoded = decode_cursor(token)
        assert decoded is not None
        assert decoded.state == "query-explore"
        assert decoded.ticks == 3
        assert decoded.bindings == {"search": {"count": 5}}

    def test_decode_invalid_cursor(self):
        """decode_cursor returns None for garbage input."""
        from keep.state_doc_runtime import decode_cursor

        assert decode_cursor("") is None
        assert decode_cursor("not-valid-base64!!!") is None
        assert decode_cursor(None) is None

    def test_stopped_flow_returns_cursor(self):
        """A budget-exhausted flow returns a cursor for resumption."""
        compiled = {
            "loop": parse_state_doc("loop", """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "test"
      limit: 5
  - then: loop
"""),
        }

        result = run_flow(
            "loop", {"query": "test"}, budget=2,
            load_state_doc=lambda n: compiled.get(n),
            run_action=lambda n, p: {"results": [], "count": 0},
        )
        assert result.status == "stopped"
        assert result.cursor is not None
        assert result.ticks == 2

    def test_resume_from_cursor(self):
        """Resuming a stopped flow continues from the checkpoint."""
        from keep.state_doc_runtime import decode_cursor

        compiled = {
            "counter": parse_state_doc("counter", """\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "test"
  - then: counter
"""),
        }

        tick_counts = []
        def runner(name, params):
            tick_counts.append(1)
            return {"results": [], "count": 0}

        # First run: budget=2
        r1 = run_flow(
            "counter", {}, budget=2,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
        )
        assert r1.status == "stopped"
        assert r1.ticks == 2
        assert r1.cursor is not None

        # Resume with budget=3
        cursor = decode_cursor(r1.cursor)
        r2 = run_flow(
            "counter", {}, budget=3,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            cursor=cursor,
        )
        assert r2.status == "stopped"
        assert r2.ticks == 5  # 2 prior + 3 new
        assert len(tick_counts) == 5  # total action calls

    def test_done_flow_no_cursor(self):
        """A completed flow does not include a cursor."""
        compiled = {
            "simple": parse_state_doc("simple", """\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "test"
  - return: done
"""),
        }

        result = run_flow(
            "simple", {}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=lambda n, p: {"results": [], "count": 0},
        )
        assert result.status == "done"
        assert result.cursor is None

    def test_bindings_accumulate_across_resume(self):
        """Bindings from previous ticks are preserved on resume."""
        from keep.state_doc_runtime import decode_cursor, FlowCursor

        compiled = {
            "accum": parse_state_doc("accum", """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "test"
  - then: accum
"""),
        }

        call_count = [0]
        def runner(name, params):
            call_count[0] += 1
            return {"results": [f"r{call_count[0]}"], "count": 1}

        r1 = run_flow(
            "accum", {}, budget=1,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
        )
        assert r1.status == "stopped"
        assert "search" in r1.bindings

        cursor = decode_cursor(r1.cursor)
        assert cursor.bindings.get("search") is not None

        r2 = run_flow(
            "accum", {}, budget=1,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            cursor=cursor,
        )
        # Bindings should have the latest search result
        assert "search" in r2.bindings


# ---------------------------------------------------------------------------
# CLI: keep flow
# ---------------------------------------------------------------------------

class TestFlowCLI:
    @pytest.fixture
    def cli(self, mock_providers, tmp_path):
        """CLI runner targeting a fresh store."""
        from keep.cli import app
        from typer.testing import CliRunner
        runner = CliRunner()
        def invoke(*args):
            env = {
                "KEEP_STORE_PATH": str(tmp_path),
                "KEEP_CONFIG": str(tmp_path),
            }
            return runner.invoke(app, list(args), env=env, catch_exceptions=False)
        return invoke

    def test_flow_help(self, cli):
        result = cli("flow", "--help")
        assert result.exit_code == 0
        assert "state-doc flow" in result.stdout.lower() or "state doc" in result.stdout.lower()

    def test_flow_requires_argument(self, cli):
        result = cli("flow")
        assert result.exit_code != 0

    def test_flow_inline_yaml(self, cli, tmp_path):
        """Run a flow from an inline YAML file."""
        state_doc = tmp_path / "test.yaml"
        state_doc.write_text("""\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "hello"
  - return: done
""")
        result = cli("flow", "--file", str(state_doc))
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["status"] == "done"

    def test_flow_with_budget(self, cli, tmp_path):
        """--budget flag limits ticks."""
        state_doc = tmp_path / "loop.yaml"
        state_doc.write_text("""\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "test"
  - then: inline
""")
        result = cli("flow", "--file", str(state_doc), "--budget", "2")
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["status"] == "stopped"
        assert output["ticks"] == 2
        assert "cursor" in output


# ---------------------------------------------------------------------------
# API: run_flow_command
# ---------------------------------------------------------------------------

class TestRunFlowCommand:
    @pytest.fixture
    def kp(self, mock_providers, tmp_path):
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        return kp

    def test_run_stored_state_doc(self, kp):
        """run_flow_command with a built-in state doc name."""
        result = kp.run_flow_command(
            "get",
            params={"id": "nonexistent"},
            budget=1,
        )
        # get-context on nonexistent ID still completes
        assert result.status in ("done", "error")

    def test_run_inline_yaml(self, kp):
        """run_flow_command with inline YAML state doc."""
        yaml_doc = """\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "hello"
  - return: done
"""
        result = kp.run_flow_command(
            "test",
            params={},
            state_doc_yaml=yaml_doc,
        )
        assert result.status == "done"
        assert result.ticks == 1

    def test_run_with_cursor_resume(self, kp):
        """run_flow_command can resume via cursor."""
        yaml_loop = """\
match: sequence
rules:
  - id: s
    do: find
    with:
      query: "test"
  - then: test
"""
        r1 = kp.run_flow_command("test", params={}, budget=1, state_doc_yaml=yaml_loop)
        assert r1.status == "stopped"
        assert r1.cursor is not None

        r2 = kp.run_flow_command("test", params={}, budget=2,
                                  cursor_token=r1.cursor, state_doc_yaml=yaml_loop)
        assert r2.status == "stopped"
        assert r2.ticks == 3  # 1 prior + 2 new


# ---------------------------------------------------------------------------
# Validation: missing params surface actionable errors
# ---------------------------------------------------------------------------

class TestFlowValidation:
    """Verify that missing required params produce error bindings, not silent nulls."""

    def test_put_missing_content(self, kp):
        r = kp.run_flow_command("put", params={}, budget=1)
        assert r.data.get("stored", {}).get("error")
        assert "content" in r.data["stored"]["error"]

    def test_tag_missing_tags(self, kp):
        r = kp.run_flow_command("tag", params={"id": "x"}, budget=1)
        tagged = r.data.get("tagged", {})
        # Action skips gracefully when no tags provided
        assert tagged.get("skipped") or tagged.get("error")

    def test_delete_missing_id(self, kp):
        r = kp.run_flow_command("delete", params={}, budget=1)
        assert r.data.get("result", {}).get("error")
        assert "id" in r.data["result"]["error"]

    def test_move_missing_name(self, kp):
        r = kp.run_flow_command("move", params={}, budget=1)
        assert r.data.get("moved", {}).get("error")
        assert "name" in r.data["moved"]["error"]

    def test_nonexistent_state_doc(self, kp):
        r = kp.run_flow_command("nonexistent", params={}, budget=1)
        assert r.status == "error"
        assert "not found" in r.data["reason"]


# ---------------------------------------------------------------------------
# Async action flag and delegation
# ---------------------------------------------------------------------------

class TestAsyncActionFlag:
    """Verify async_action decorator flag and is_async_action() lookup."""

    def test_async_actions_marked(self):
        from keep.actions import is_async_action
        assert is_async_action("summarize") is True
        assert is_async_action("generate") is True
        assert is_async_action("analyze") is True
        assert is_async_action("auto_tag") is True
        assert is_async_action("ocr") is True
        assert is_async_action("describe") is True

    def test_sync_actions_not_marked(self):
        from keep.actions import is_async_action
        assert is_async_action("find") is False
        assert is_async_action("get") is False
        assert is_async_action("put") is False
        assert is_async_action("traverse") is False
        assert is_async_action("resolve_meta") is False
        assert is_async_action("extract_links") is False
        assert is_async_action("tag") is False

    def test_unknown_action_returns_false(self):
        from keep.actions import is_async_action
        assert is_async_action("nonexistent") is False
        assert is_async_action("") is False


class TestAsyncDelegation:
    """Verify that foreground flows delegate async actions via cursor."""

    def test_foreground_flow_stops_at_async_action(self):
        """Foreground flow hitting an async action → status='async' + cursor."""
        compiled = {
            "mixed": parse_state_doc("mixed", """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "test"
  - id: summary
    do: summarize
    with:
      item_id: "test-item"
  - return: done
"""),
        }

        calls = []
        def runner(name, params):
            calls.append(name)
            return {"results": [], "count": 0}

        result = run_flow(
            "mixed", {}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            foreground=True,  # default, but explicit
        )

        assert result.status == "async"
        assert result.cursor is not None
        # find executed, summarize did not
        assert calls == ["find"]
        assert result.data["action"] == "summarize"

    def test_daemon_flow_executes_async_actions_inline(self):
        """Daemon flow (foreground=False) executes async actions inline."""
        compiled = {
            "mixed": parse_state_doc("mixed", """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "test"
  - id: summary
    do: summarize
    with:
      item_id: "test-item"
  - return: done
"""),
        }

        calls = []
        def runner(name, params):
            calls.append(name)
            return {"results": [], "count": 0}

        result = run_flow(
            "mixed", {}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            foreground=False,
        )

        assert result.status == "done"
        assert result.cursor is None
        # Both actions executed
        assert calls == ["find", "summarize"]

    def test_async_cursor_carries_state(self):
        """Cursor from async delegation encodes state for resume."""
        from keep.state_doc_runtime import decode_cursor

        compiled = {
            "two-step": parse_state_doc("two-step", """\
match: sequence
rules:
  - id: prep
    do: find
    with:
      query: "prep"
  - id: gen
    do: generate
    with:
      system: "test"
      user: "test"
  - return: done
"""),
        }

        result = run_flow(
            "two-step", {"context": "test"}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=lambda n, p: {"results": [], "count": 0, "text": "ok"},
            foreground=True,
        )

        assert result.status == "async"
        cursor = decode_cursor(result.cursor)
        assert cursor is not None
        assert cursor.state == "two-step"

    def test_async_resume_completes_flow(self):
        """Resume from async cursor in daemon context completes the flow."""
        from keep.state_doc_runtime import decode_cursor

        compiled = {
            "async-resume": parse_state_doc("async-resume", """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "test"
  - id: gen
    do: generate
    with:
      system: "test"
      user: "test"
  - return: done
"""),
        }

        calls = []
        def runner(name, params):
            calls.append(name)
            return {"results": [], "count": 0, "text": "generated"}

        # Step 1: foreground stops at generate
        r1 = run_flow(
            "async-resume", {}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            foreground=True,
        )
        assert r1.status == "async"
        assert calls == ["find"]

        # Step 2: resume in daemon context
        cursor = decode_cursor(r1.cursor)
        calls.clear()
        r2 = run_flow(
            "async-resume", {}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            cursor=cursor,
            foreground=False,
        )
        assert r2.status == "done"
        # Re-evaluates from top: find runs again, then generate completes
        assert "find" in calls
        assert "generate" in calls

    def test_sync_only_flow_unaffected_by_foreground(self):
        """Flow with only sync actions completes normally in foreground."""
        compiled = {
            "sync-only": parse_state_doc("sync-only", """\
match: sequence
rules:
  - id: s1
    do: find
    with:
      query: "a"
  - id: s2
    do: get
    with:
      id: "test"
  - return: done
"""),
        }

        calls = []
        def runner(name, params):
            calls.append(name)
            return {"results": [], "count": 0, "id": "test", "summary": "x"}

        result = run_flow(
            "sync-only", {}, budget=5,
            load_state_doc=lambda n: compiled.get(n),
            run_action=runner,
            foreground=True,
        )

        assert result.status == "done"
        assert calls == ["find", "get"]


# ---------------------------------------------------------------------------
# Flow work item execution (daemon)
# ---------------------------------------------------------------------------

class TestFlowWorkItemExecution:
    """Verify _execute_flow_item handles fresh and cursor-based flows."""

    def test_fresh_flow_execution(self, kp):
        """_execute_flow_item runs a fresh flow (no cursor)."""
        from keep.work_processor import _execute_flow_item

        kp.put("Test content for flow", id="flow-test-1")

        result = _execute_flow_item(kp, {
            "state": "get",
            "params": {
                "item_id": "flow-test-1",
                "similar_limit": 3,
                "meta_limit": 3,
                "parts_limit": 5,
                "versions_limit": 3,
                "edges_limit": 5,
            },
        })

        assert result["status"] in ("done", "error")

    def test_flow_item_requires_state(self):
        """Flow work item without state name raises ValueError."""
        from keep.work_processor import _execute_flow_item
        from keep.api import Keeper

        with pytest.raises(ValueError, match="missing state name"):
            _execute_flow_item(None, {"params": {}})

    def test_after_write_flow_item_has_correct_shape(self, kp):
        """After-write enqueues a flow item with expected fields."""
        queue = kp._get_work_queue()
        queue.claim("drain", limit=200)

        kp.put("Content for shape test " * 30, id="shape-1")

        claimed = queue.claim("test", limit=10)
        flow_items = [i for i in claimed if i.kind == "flow"]
        assert len(flow_items) == 1

        item = flow_items[0]
        assert item.input["state"] == "after-write"
        assert item.input["item_id"] == "shape-1"
        assert "content" in item.input
        params = item.input["params"]
        assert params["item_id"] == "shape-1"
        assert "item" in params
        assert params["item"]["has_content"] is True
        assert params["item"]["content_length"] > 0
        assert "system" in params
        assert "max_summary_length" in params


class TestEnqueueFlowCursor:
    """Verify _enqueue_flow_cursor puts cursor into work queue."""

    def test_enqueue_and_claim_cursor(self, kp):
        """Enqueued cursor appears as a 'flow' work item."""
        from keep.state_doc_runtime import encode_cursor

        queue = kp._get_work_queue()
        queue.claim("drain", limit=200)

        cursor_token = encode_cursor("test-state", 1, {"s": {"count": 5}})
        kp._enqueue_flow_cursor(
            state="test-state",
            cursor_token=cursor_token,
            params={"item_id": "x"},
            priority=5,
        )

        claimed = queue.claim("test", limit=10)
        flow_items = [i for i in claimed if i.kind == "flow"]
        assert len(flow_items) == 1
        assert flow_items[0].input["state"] == "test-state"
        assert flow_items[0].input["cursor"] == cursor_token
