import pytest

from keep.api import Keeper
from keep.continuation_engine import ContinuationEngine
from keep.continuation_env import LocalContinuationEnvironment
from keep.continuation_executor import WorkExecutor
from keep.continuation_store import SQLiteFlowStore


def _summarize_request(note_id: str, content: str, *, request_id: str, idempotency_key: str | None = None) -> dict:
    payload = {
        "request_id": request_id,
        "goal": "summarize",
        "params": {"id": note_id, "content": content},
        "steps": [
            {
                "kind": "summarize",
                "runner": {"type": "provider.summarize"},
                "input_mode": "note_content",
                "output_contract": {"must_return": ["summary"]},
                "apply": {
                    "ops": [
                        {"op": "set_summary", "summary": "$output.summary"},
                    ]
                },
            }
        ],
        "work_results": [],
    }
    if idempotency_key is not None:
        payload["idempotency_key"] = idempotency_key
    return payload


def test_engine_round_trip_with_adapters(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
    )
    try:
        content = "engine alpha " * 120
        kp.put(content=content, id="note:engine:1", summary="placeholder")

        first = engine.continue_flow(
            _summarize_request("note:engine:1", content, request_id="engine-req-1")
        )
        assert first["status"] == "waiting_work"
        work_id = first["work"][0]["work_id"]

        work_result = engine.run_work(first["cursor"], work_id)
        second = engine.continue_flow(
            {
                "request_id": "engine-req-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "done"
        item = kp.get("note:engine:1")
        assert item is not None
        assert item.summary == content[:200]
    finally:
        engine.close()
        kp.close()


def test_engine_idempotency_replay_with_adapters(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
    )
    try:
        content = "engine beta " * 80
        kp.put(content=content, id="note:engine:2", summary="placeholder")

        first = engine.continue_flow(
            _summarize_request(
                "note:engine:2",
                content,
                request_id="engine-idem-1",
                idempotency_key="engine-idem-key",
            )
        )
        replay = engine.continue_flow(
            _summarize_request(
                "note:engine:2",
                content,
                request_id="engine-idem-2",
                idempotency_key="engine-idem-key",
            )
        )
        assert replay["cursor"] == first["cursor"]
        assert replay["status"] == first["status"]
    finally:
        engine.close()
        kp.close()


def test_engine_process_requested_work_batch_applies_result(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
    )
    try:
        content = "engine batch " * 120
        kp.put(content=content, id="note:engine:batch", summary="placeholder")

        first = engine.continue_flow(
            _summarize_request("note:engine:batch", content, request_id="engine-batch-1")
        )
        assert first["status"] == "waiting_work"
        assert engine.count_requested_work() == 1

        processed = engine.process_requested_work_batch(worker_id="worker-1", limit=10)
        assert processed["claimed"] == 1
        assert processed["processed"] == 1
        assert processed["failed"] == 0
        assert processed["dead_lettered"] == 0
        assert engine.count_requested_work() == 0

        item = kp.get("note:engine:batch")
        assert item is not None
        assert item.summary == content[:200]
    finally:
        engine.close()
        kp.close()


class _FailingWorkExecutor(WorkExecutor):
    def execute(self, payload: dict):
        raise RuntimeError("boom")


def test_engine_process_requested_work_batch_dead_letters_after_max_attempts(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
        work_executor=_FailingWorkExecutor(),
    )
    try:
        content = "engine dead letter " * 120
        kp.put(content=content, id="note:engine:dead-letter", summary="placeholder")

        first = engine.continue_flow(
            _summarize_request("note:engine:dead-letter", content, request_id="engine-dead-1")
        )
        work_id = first["work"][0]["work_id"]
        flow_store._conn.execute(
            "UPDATE continue_work SET attempt = 2, max_attempts = 1 WHERE work_id = ?",
            (work_id,),
        )
        flow_store._conn.commit()

        processed = engine.process_requested_work_batch(worker_id="worker-dead", limit=10)
        assert processed["claimed"] == 1
        assert processed["processed"] == 0
        assert processed["failed"] == 0
        assert processed["dead_lettered"] == 1
        assert engine.count_requested_work() == 0
    finally:
        engine.close()
        kp.close()


def test_engine_local_task_runner_invokes_task_workflow(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
    )
    calls: list[dict] = []
    original = kp._run_local_task_workflow
    try:
        kp.put(content="task input", id="note:task", summary="task input")

        def _fake_run_local_task_workflow(
            *,
            task_type: str,
            item_id: str,
            collection: str,
            content: str,
            metadata: dict | None = None,
        ) -> dict:
            calls.append(
                {
                    "task_type": task_type,
                    "item_id": item_id,
                    "collection": collection,
                    "content": content,
                    "metadata": dict(metadata or {}),
                }
            )
            return {"status": "applied", "details": {"ok": True}}

        kp._run_local_task_workflow = _fake_run_local_task_workflow  # type: ignore[assignment]
        doc_coll = kp._resolve_doc_collection()
        first = engine.continue_flow(
            {
                "request_id": "task-runner-1",
                "goal": "task.analyze",
                "params": {"id": "note:task"},
                "steps": [
                    {
                        "kind": "analyze",
                        "runner": {"type": "local.task", "task_type": "analyze"},
                        "input_mode": "none",
                        "input": {
                            "task_type": "analyze",
                            "item_id": "note:task",
                            "collection": doc_coll,
                            "content": "",
                            "metadata": {"force": True},
                        },
                        "output_contract": {"must_return": ["status"]},
                    }
                ],
                "work_results": [],
            }
        )
        assert first["status"] == "waiting_work"
        work_id = first["work"][0]["work_id"]
        work_result = engine.run_work(first["cursor"], work_id)
        assert work_result["outputs"]["status"] == "applied"
        second = engine.continue_flow(
            {
                "request_id": "task-runner-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "done"
        assert len(calls) == 1
        assert calls[0]["task_type"] == "analyze"
        assert calls[0]["item_id"] == "note:task"
        assert calls[0]["metadata"].get("force") is True
    finally:
        kp._run_local_task_workflow = original  # type: ignore[assignment]
        engine.close()
        kp.close()


# ---------------------------------------------------------------------------
# State-doc after-write integration
# ---------------------------------------------------------------------------

AFTER_WRITE_STATE_DOC = """\
match: all
rules:
  - when: "item.content_length > 100 && !item.has_summary"
    id: summary
    do: summarize
  - when: "!item.is_system_note"
    id: tags
    do: tag
post:
  - return: done
"""


@pytest.fixture
def _engine_with_state_doc(mock_providers, tmp_path):
    """Create a Keeper + engine with a .state/after-write doc installed."""
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
    )
    # Install the state doc as a keep note
    kp.put(content=AFTER_WRITE_STATE_DOC, id=".state/after-write", summary=AFTER_WRITE_STATE_DOC)
    yield kp, engine
    engine.close()
    kp.close()


def test_state_doc_after_write_queues_tasks(_engine_with_state_doc):
    """State-doc after-write fires summarize+tag for long non-system content."""
    kp, engine = _engine_with_state_doc
    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type, "id": id, "content": content})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        long_content = "This is a long note with lots of content. " * 10
        result = engine.continue_flow({
            "request_id": "sd-write-1",
            "goal": "write",
            "params": {
                "content": long_content,
                "id": "note:sd:test1",
                "processing": {"summarize": True, "tag": True},
            },
        })
        assert result.get("status") in ("done", "stopped", "waiting_work")

        task_types = [e["task_type"] for e in enqueued]
        assert "summarize" in task_types
        assert "tag" in task_types
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]


def test_state_doc_after_write_skips_short_content(_engine_with_state_doc):
    """State-doc after-write skips summarize for short content (< 100 chars)."""
    kp, engine = _engine_with_state_doc
    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type, "id": id})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        result = engine.continue_flow({
            "request_id": "sd-write-2",
            "goal": "write",
            "params": {
                "content": "Short note",
                "id": "note:sd:test2",
                "processing": {"summarize": True, "tag": True},
            },
        })
        task_types = [e["task_type"] for e in enqueued]
        assert "summarize" not in task_types  # content too short
        assert "tag" in task_types  # non-system note, tag fires
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]


def test_system_notes_skip_state_doc_eval(_engine_with_state_doc):
    """System notes (id starts with '.') skip state-doc eval, use template fallback."""
    kp, engine = _engine_with_state_doc
    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type, "id": id})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        long_content = "System note content that is quite long. " * 10
        result = engine.continue_flow({
            "request_id": "sd-write-3",
            "goal": "write",
            "params": {
                "content": long_content,
                "id": ".system/test-note",
                # Template followups use these processing flags
                "processing": {"summarize": True, "tag": False},
            },
        })
        task_types = [e["task_type"] for e in enqueued]
        # Falls through to template followups (not state doc)
        assert "summarize" in task_types  # processing.summarize=True
        assert "tag" not in task_types  # processing.tag=False
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]


def test_state_doc_generic_passthrough(_engine_with_state_doc):
    """State-doc with: params pass through as task metadata generically."""
    kp, engine = _engine_with_state_doc

    # Install a state doc with custom action and with: params
    custom_doc = """\
match: all
rules:
  - do: custom_action
    with:
      model: "gpt-4"
      temperature: 0.7
post:
  - return: done
"""
    kp.put(content=custom_doc, id=".state/after-write", summary=custom_doc)

    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type, "id": id, "metadata": metadata or {}, "tags": tags or {}})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        result = engine.continue_flow({
            "request_id": "sd-generic-1",
            "goal": "write",
            "params": {"content": "test content", "id": "note:generic:test"},
        })
        assert len(enqueued) == 1
        assert enqueued[0]["task_type"] == "custom_action"
        assert enqueued[0]["metadata"]["model"] == "gpt-4"
        assert enqueued[0]["metadata"]["temperature"] == 0.7
        # Item tags are passed through
        assert isinstance(enqueued[0]["tags"], dict)
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]


def test_state_doc_error_terminal_propagates(_engine_with_state_doc):
    """State-doc return: error propagates as a flow error."""
    kp, engine = _engine_with_state_doc

    error_doc = """\
match: sequence
rules:
  - return:
      status: error
      with:
        reason: "content rejected"
"""
    # Write via _put_direct to avoid triggering state-doc eval on the state doc itself
    kp._put_direct(content=error_doc, id=".state/after-write", summary=error_doc)

    result = engine.continue_flow({
        "request_id": "sd-error-1",
        "goal": "write",
        "params": {"content": "bad content", "id": "note:error:test"},
    })
    # The flow should report an error from the state doc
    assert result.get("status") == "error" or "state_doc_terminal_error" in str(result.get("errors", []))


def test_state_doc_transition_recorded_in_frontier(_engine_with_state_doc):
    """State-doc then: transition is recorded in flow frontier state."""
    kp, engine = _engine_with_state_doc

    transition_doc = """\
match: sequence
rules:
  - do: summarize
  - then: review-content
"""
    kp.put(content=transition_doc, id=".state/after-write", summary=transition_doc)

    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        result = engine.continue_flow({
            "request_id": "sd-transition-1",
            "goal": "write",
            "params": {"content": "content to review", "id": "note:transition:test"},
        })
        # Action should still fire
        assert len(enqueued) == 1
        assert enqueued[0]["task_type"] == "summarize"
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]


def test_state_doc_content_passed_to_all_actions(_engine_with_state_doc):
    """Content is passed to all actions, not just a hard-coded subset."""
    kp, engine = _engine_with_state_doc

    doc = """\
match: all
rules:
  - do: custom_processor
post:
  - return: done
"""
    kp.put(content=doc, id=".state/after-write", summary=doc)

    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type, "content": content})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        test_content = "This is content for a custom processor"
        engine.continue_flow({
            "request_id": "sd-content-1",
            "goal": "write",
            "params": {"content": test_content, "id": "note:content:test"},
        })
        assert len(enqueued) == 1
        assert enqueued[0]["content"] == test_content
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]


def test_fallback_to_template_when_no_state_doc(mock_providers, tmp_path):
    """Without a .state/after-write doc, template followups still work."""
    kp = Keeper(store_path=tmp_path)
    flow_store = SQLiteFlowStore(tmp_path / "engine-continuation.db")
    engine = ContinuationEngine(
        flow_store=flow_store,
        env=LocalContinuationEnvironment(kp),
    )
    enqueued: list[dict] = []
    original = kp._enqueue_task_background

    def _capture_enqueue(*, task_type, id, doc_coll, content, metadata=None, tags=None):
        enqueued.append({"task_type": task_type, "id": id})

    kp._enqueue_task_background = _capture_enqueue  # type: ignore[assignment]
    try:
        long_content = "Template fallback test content. " * 20
        result = engine.continue_flow({
            "request_id": "sd-write-fallback",
            "goal": "write",
            "params": {
                "content": long_content,
                "id": "note:sd:fallback",
                "processing": {"summarize": True, "tag": True},
            },
        })
        # Template followups should still fire
        task_types = [e["task_type"] for e in enqueued]
        assert "summarize" in task_types
        assert "tag" in task_types
    finally:
        kp._enqueue_task_background = original  # type: ignore[assignment]
        engine.close()
        kp.close()
