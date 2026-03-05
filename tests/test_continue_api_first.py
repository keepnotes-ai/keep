import json

from keep.api import Keeper


def _summarize_request(note_id: str, content: str, *, request_id: str, idempotency_key: str | None = None) -> dict:
    payload = {
        "schema_version": "continue.v1",
        "request_id": request_id,
        "goal": "summarize",
        "params": {"id": note_id, "content": content},
        "steps": [
            {
                "kind": "summarize",
                "runner": {"type": "provider.summarize"},
                "input_mode": "note_content",
                "output_contract": {"must_return": ["summary"], "schema_version": "1.0"},
                "apply": {
                    "ops": [
                        {"op": "set_summary", "summary": "$output.summary"},
                    ]
                },
            }
        ],
        "feedback": {"work_results": []},
    }
    if idempotency_key is not None:
        payload["idempotency_key"] = idempotency_key
    return payload


def test_continue_summarize_round_trip(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "alpha " * 120
        kp.put(content=content, id="note:1", summary="placeholder")

        first = kp.continue_flow(
            _summarize_request(
                "note:1",
                content,
                request_id="req-1",
                idempotency_key="idem-1",
            )
        )
        assert first["status"] == "waiting_work"
        assert len(first["requests"]["work"]) == 1
        work_id = first["requests"]["work"][0]["work_id"]

        work_result = kp.continue_run_work(first["flow_id"], work_id)
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": [work_result]},
            }
        )
        assert second["status"] == "done"
        item = kp.get("note:1")
        assert item is not None
        assert item.summary != "placeholder"
        assert item.summary == content[:200]
    finally:
        kp.close()


def test_continue_idempotency_replay_is_stable(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "beta " * 100
        kp.put(content=content, id="note:2", summary="placeholder")

        first = kp.continue_flow(
            _summarize_request(
                "note:2",
                content,
                request_id="req-1",
                idempotency_key="idem-replay",
            )
        )
        replay = kp.continue_flow(
            _summarize_request(
                "note:2",
                content,
                request_id="req-2",
                idempotency_key="idem-replay",
            )
        )
        assert replay["flow_id"] == first["flow_id"]
        assert replay["state_version"] == first["state_version"]
        assert replay["status"] == first["status"]
    finally:
        kp.close()


def test_continue_state_conflict_returns_error(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "gamma " * 100
        kp.put(content=content, id="note:3", summary="placeholder")

        first = kp.continue_flow(
            _summarize_request("note:3", content, request_id="req-1")
        )
        stale = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"] - 1,
                "feedback": {"work_results": []},
            }
        )
        assert stale["status"] == "failed"
        assert stale["errors"]
        assert stale["errors"][0]["code"] == "state_conflict"
    finally:
        kp.close()


def test_continue_frame_request_id_seed(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="frame content", id="note:frame", summary="frame summary")
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-frame-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:frame"},
                    "budget": {"max_nodes": 5},
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        evidence = out["frame"]["views"]["evidence"]
        assert evidence
        assert evidence[0]["id"] == "note:frame"
    finally:
        kp.close()


def test_continue_template_query_renders_text(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="auth migration notes", id="note:q1", summary="auth migration notes")
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-template-1",
                "template_ref": ".prompt/agent/query",
                "params": {"text": "auth migration"},
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        assert "rendered" in out
        assert out["rendered"]["template_ref"] == ".prompt/agent/query"
        assert isinstance(out["rendered"]["text"], str)
        assert len(out["rendered"]["text"]) > 0
    finally:
        kp.close()


def test_continue_template_bindings_compile_and_render(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="binding context note", id="note:bind-1", summary="binding context note")
        template = """---
bindings:
  question:
    from: params.text
  context_block:
    frame_request:
      seed:
        mode: query
        value: "${params.text}"
      pipeline:
        - op: slice
          args:
            limit: 2
render: |
  Q: {{question}}
  C:
  {{context_block}}
---
# .prompt/agent/bind-demo
"""
        kp.put(content=template, id=".prompt/agent/bind-demo", summary=template)
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-bind-1",
                "template_ref": ".prompt/agent/bind-demo",
                "params": {"text": "binding context"},
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        assert "rendered" in out
        assert "Q: binding context" in out["rendered"]["text"]
        assert "binding context note" in out["rendered"]["text"]
        assert out["frame"]["slots"]["question"] == "binding context"
        assert "context_block" in out["frame"]["slots"]
    finally:
        kp.close()


def test_continue_program_persists_across_ticks(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="release plan and risks", id="note:persist", summary="release plan and risks")
        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-persist-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "query", "value": "release plan"},
                    "budget": {"max_nodes": 5},
                },
                "feedback": {"work_results": []},
            }
        )
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-persist-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": []},
            }
        )
        assert second["frame"]["slots"]["goal"] == "query"
        assert second["state"]["program"]["goal"] == "query"
    finally:
        kp.close()


def test_continue_query_auto_profile_schedules_and_consumes_refine(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="alpha memory A", id="note:auto-1", tags={"conv": "A"}, summary="alpha memory A")
        kp.put(content="alpha memory A2", id="note:auto-2", tags={"conv": "A"}, summary="alpha memory A2")
        kp.put(content="alpha memory B", id="note:auto-3", tags={"conv": "B"}, summary="alpha memory B")

        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-auto-1",
                "goal": "query",
                "profile": "query.auto",
                "params": {"text": "alpha memory"},
                "frame_request": {
                    "seed": {"mode": "query", "value": "alpha memory"},
                    "pipeline": [{"op": "slice", "args": {"limit": 5}}],
                    "options": {"metadata": "basic"},
                },
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "done"
        assert first["next"]["recommended"] == "continue"
        assert "auto_query_next_frame_request" in first["state"]["frontier"]

        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-auto-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": []},
            }
        )
        assert second["status"] == "done"
        assert second["state"]["frontier"].get("auto_query_refined") is True
        assert "auto_query_next_frame_request" not in second["state"]["frontier"]
        effective_frame = second["state"]["program"]["frame_request"]
        assert effective_frame["options"]["deep"] is True
    finally:
        kp.close()


def test_continue_query_auto_profile_single_lane_refine_adds_where(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="focus result one", id="note:auto-s1", tags={"conv": "7"}, summary="focus result one")
        kp.put(content="focus result two", id="note:auto-s2", tags={"conv": "7"}, summary="focus result two")
        kp.put(content="focus result other", id="note:auto-s3", tags={"conv": "3"}, summary="focus result other")

        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-auto-lane-1",
                "goal": "query",
                "profile": "query.auto",
                "params": {"text": "focus result"},
                "frame_request": {
                    "seed": {"mode": "query", "value": "focus result"},
                    "pipeline": [{"op": "slice", "args": {"limit": 5}}],
                    "options": {"metadata": "basic"},
                },
                "decision_override": {
                    "strategy": "single_lane_refine",
                    "reason": "test_single_lane",
                },
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "done"
        pending = first["state"]["frontier"].get("auto_query_next_frame_request")
        assert isinstance(pending, dict)
        pipeline = pending.get("pipeline")
        assert isinstance(pipeline, list)
        assert pipeline
        assert pipeline[0].get("op") == "where"
        facts = pipeline[0].get("args", {}).get("facts", [])
        assert facts
    finally:
        kp.close()


def test_continue_profile_stages_drive_process_progression(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "stage progression sample"
        kp.put(content=content, id="note:stages", summary=content)
        profile_doc = json.dumps(
            {
                "stages": [
                    {
                        "name": "classify",
                        "emits_work": "classify_step",
                        "runner": {"type": "echo", "outputs": {"labels": {"phase": "classified"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    },
                    {
                        "name": "finalize",
                        "terminal": True,
                    },
                ]
            }
        )
        kp.put(content=profile_doc, id=".profile/stage.demo", summary=profile_doc)

        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-stage-1",
                "goal": "pipeline",
                "profile": "stage.demo",
                "params": {"id": "note:stages", "content": content},
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "waiting_work"
        assert first["state"]["cursor"]["stage"] == "classify"
        assert first["requests"]["work"][0]["kind"] == "classify_step"

        work_result = kp.continue_run_work(first["flow_id"], first["requests"]["work"][0]["work_id"])
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-stage-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": [work_result]},
            }
        )
        assert second["status"] == "done"
        assert second["state"]["cursor"]["stage"] == "finalize"
        assert second["state"]["cursor"]["phase"] == "reconcile"
        tagged = kp.get("note:stages")
        assert tagged is not None
        assert tagged.tags.get("phase") == "classified"
    finally:
        kp.close()


def test_continue_invalid_frame_operator_fails(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-op-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "query", "value": "anything"},
                    "pipeline": [{"op": "explode", "args": {}}],
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "failed"
        assert out["errors"]
        assert out["errors"][0]["code"] == "invalid_frame_operator"
    finally:
        kp.close()


def test_continue_frame_evidence_includes_basic_metadata(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="meta evidence", id="note:meta", tags={"topic": "continuations"}, summary="meta evidence")
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-meta-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:meta"},
                    "options": {"metadata": "basic"},
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        evidence = out["frame"]["views"]["evidence"]
        assert evidence
        metadata = evidence[0]["metadata"]
        assert metadata["level"] == "basic"
        assert metadata["base_id"] == "note:meta"
        assert metadata["tags"]["topic"] == "continuations"
    finally:
        kp.close()


def test_continue_frame_evidence_includes_rich_metadata(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="rich metadata sample", id="note:meta-rich", summary="rich metadata sample")
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-meta-rich-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:meta-rich"},
                    "options": {"metadata": "rich"},
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        evidence = out["frame"]["views"]["evidence"]
        assert evidence
        metadata = evidence[0]["metadata"]
        assert metadata["level"] == "rich"
        assert "structure" in metadata
        assert "parts" in metadata["structure"]
        assert "links" in metadata
    finally:
        kp.close()


def test_continue_publishes_decision_discriminators_and_snapshot(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(
            content="decision capsule basis",
            id="note:decision",
            tags={"topic": "continuations", "speaker": "alice"},
            summary="decision capsule basis",
        )
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-decision-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:decision"},
                    "options": {"metadata": "basic"},
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        discriminators = out["frame"]["views"]["discriminators"]
        assert discriminators["version"] == "ds.v1"
        assert set(discriminators["planner_priors"].keys()) == {"fanout", "selectivity", "cardinality"}
        assert set(discriminators["query_stats"].keys()) == {
            "lane_entropy",
            "top1_top2_margin",
            "pivot_coverage_topk",
            "expansion_yield_prev_step",
            "cost_per_gain_prev_step",
            "temporal_alignment",
        }
        assert "policy_hint" in discriminators
        snapshot = out["state"]["frontier"]["decision_support"]
        assert set(snapshot.keys()) == {"version", "strategy_chosen", "reason_codes", "pivot_ids"}
        assert snapshot["version"] == "ds.v1"
        assert "query_stats" not in snapshot
    finally:
        kp.close()


def test_continue_decision_override_controls_strategy(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="override strategy", id="note:override", summary="override strategy")
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-decision-override-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:override"},
                    "options": {"metadata": "basic"},
                },
                "decision_override": {
                    "strategy": "top2_plus_bridge",
                    "reason": "cross-lane question",
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        policy_hint = out["frame"]["views"]["discriminators"]["policy_hint"]
        assert policy_hint["strategy"] == "top2_plus_bridge"
        assert "override:cross-lane question" in policy_hint["reason_codes"]
        snapshot = out["state"]["frontier"]["decision_support"]
        assert snapshot["strategy_chosen"] == "top2_plus_bridge"
    finally:
        kp.close()


def test_continue_custom_steps_applies_tags(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "Any document content works here."
        kp.put(content=content, id="note:classify", summary=content)
        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-classify-1",
                "goal": "classify",
                "params": {"id": "note:classify", "content": content},
                "steps": [
                    {
                        "kind": "classify",
                        "runner": {
                            "type": "echo",
                            "outputs": {
                                "labels": {
                                    "source_quality": "high",
                                    "sentiment_hint": "neutral",
                                }
                            },
                        },
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {
                            "ops": [
                                {"op": "set_tags", "tags": "$output.labels"},
                            ]
                        },
                    }
                ],
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "waiting_work"
        assert first["requests"]["work"]
        assert first["requests"]["work"][0]["kind"] == "classify"

        work_result = kp.continue_run_work(
            first["flow_id"], first["requests"]["work"][0]["work_id"],
        )
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-classify-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": [work_result]},
            }
        )
        assert second["status"] == "done"
        item = kp.get("note:classify")
        assert item is not None
        assert item.tags.get("source_quality") == "high"
        assert item.tags.get("sentiment_hint") == "neutral"
    finally:
        kp.close()


def test_continue_work_request_includes_quality_gates_and_escalation(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "quality gate sample"
        kp.put(content=content, id="note:quality", summary=content)
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-quality-1",
                "goal": "pipeline",
                "params": {"id": "note:quality", "content": content},
                "steps": [
                    {
                        "kind": "quality_step",
                        "runner": {"type": "echo", "outputs": {"ok": True}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["ok"], "schema_version": "1.0"},
                        "quality_gates": {"min_confidence": 0.7, "citation_required": True},
                        "escalate_if": ["low_confidence", "missing_citation"],
                    }
                ],
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "waiting_work"
        assert out["requests"]["work"]
        work = out["requests"]["work"][0]
        assert work["quality_gates"] == {"min_confidence": 0.7, "citation_required": True}
        assert work["escalate_if"] == ["low_confidence", "missing_citation"]
    finally:
        kp.close()


def test_continue_set_tags_preserves_list_values(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "list tag example"
        kp.put(content=content, id="note:list-tags", summary=content)
        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-list-tags-1",
                "goal": "classify",
                "params": {"id": "note:list-tags", "content": content},
                "steps": [
                    {
                        "kind": "classify",
                        "runner": {
                            "type": "echo",
                            "outputs": {"labels": {"topic": ["continuations", "api"]}},
                        },
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    }
                ],
                "feedback": {"work_results": []},
            }
        )
        work_result = kp.continue_run_work(
            first["flow_id"], first["requests"]["work"][0]["work_id"],
        )
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-list-tags-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": [work_result]},
            }
        )
        assert second["status"] == "done"
        item = kp.get("note:list-tags")
        assert item is not None
        assert item.tags.get("topic") == ["continuations", "api"]
    finally:
        kp.close()


def test_continue_inline_write_single_tick(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-write-1",
                "goal": "write",
                "params": {"id": "note:write", "content": "hello", "tags": {"topic": "demo"}},
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        assert out["requests"]["work"] == []
        item = kp.get("note:write")
        assert item is not None
        assert item.summary
        assert item.tags.get("topic") == "demo"
    finally:
        kp.close()


def test_continue_multi_step_plan_orders_work(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "This report has both risk and successful mitigation details."
        kp.put(content=content, id="note:ordered", summary="placeholder")
        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-order-1",
                "goal": "pipeline",
                "params": {
                    "id": "note:ordered",
                    "content": content,
                },
                "steps": [
                    {
                        "kind": "classify",
                        "runner": {
                            "type": "echo",
                            "outputs": {"labels": {"doc_phase": "classified"}},
                        },
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {
                            "ops": [
                                {"op": "set_tags", "tags": "$output.labels"},
                            ]
                        },
                    },
                    {
                        "kind": "condense",
                        "when": {"work_completed": "classify"},
                        "runner": {"type": "provider.summarize"},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["summary"], "schema_version": "1.0"},
                        "apply": {
                            "ops": [
                                {"op": "set_summary", "summary": "$output.summary"},
                            ]
                        },
                    },
                ],
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "waiting_work"
        assert first["requests"]["work"][0]["kind"] == "classify"

        classify_result = kp.continue_run_work(
            first["flow_id"], first["requests"]["work"][0]["work_id"],
        )
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-order-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": [classify_result]},
            }
        )
        assert second["status"] == "done"
        tagged = kp.get("note:ordered")
        assert tagged is not None
        assert tagged.tags.get("doc_phase") == "classified"

        third = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-order-3",
                "flow_id": second["flow_id"],
                "state_version": second["state_version"],
                "feedback": {"work_results": []},
            }
        )
        assert third["status"] == "waiting_work"
        assert third["requests"]["work"][0]["kind"] == "condense"
    finally:
        kp.close()


def test_continue_profile_steps_are_loaded(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "profile-driven work"
        kp.put(content=content, id="note:profile", summary=content)
        profile_doc = json.dumps(
            {
                "steps": [
                    {
                        "kind": "profile_step",
                        "runner": {"type": "echo", "outputs": {"labels": {"profile_loaded": "yes"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    }
                ]
            }
        )
        kp.put(content=profile_doc, id=".profile/demo.profile", summary=profile_doc)

        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-profile-1",
                "goal": "pipeline",
                "profile": "demo.profile",
                "params": {"id": "note:profile", "content": content},
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "waiting_work"
        assert first["requests"]["work"][0]["kind"] == "profile_step"
    finally:
        kp.close()


def test_continue_parallel_work_emission(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "parallel content"
        kp.put(content=content, id="note:parallel", summary=content)
        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-parallel-1",
                "goal": "pipeline",
                "params": {"id": "note:parallel", "content": content},
                "steps": [
                    {
                        "kind": "a",
                        "runner": {"type": "echo", "outputs": {"labels": {"a": "1"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    },
                    {
                        "kind": "b",
                        "runner": {"type": "echo", "outputs": {"labels": {"b": "1"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    },
                ],
                "feedback": {"work_results": []},
            }
        )
        assert first["status"] == "waiting_work"
        kinds = sorted([w["kind"] for w in first["requests"]["work"]])
        assert kinds == ["a", "b"]
    finally:
        kp.close()


def test_continue_params_steps_is_ignored(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "params steps should not run"
        kp.put(content=content, id="note:param-plan", summary=content)
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-param-plan-1",
                "goal": "pipeline",
                "params": {
                    "id": "note:param-plan",
                    "content": content,
                    "steps": [
                        {
                            "kind": "should_not_emit",
                            "runner": {"type": "echo", "outputs": {"labels": {"x": "1"}}},
                            "input_mode": "note_content",
                            "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                            "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                        }
                    ],
                },
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "done"
        assert out["requests"]["work"] == []
    finally:
        kp.close()


def test_continue_rejects_legacy_program_fields(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        out = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-legacy-1",
                "intent": {"goal": "query"},
                "frame_request": {"seed": {"mode": "query", "value": "anything"}},
                "feedback": {"work_results": []},
            }
        )
        assert out["status"] == "failed"
        assert out["errors"]
        assert out["errors"][0]["code"] == "invalid_input"
        assert "Unsupported legacy continuation fields" in out["errors"][0]["message"]
    finally:
        kp.close()


def test_continue_rejects_apply_from_output(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "from_output compatibility is removed"
        kp.put(content=content, id="note:from-output", summary=content)
        first = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-from-output-1",
                "goal": "classify",
                "params": {"id": "note:from-output", "content": content},
                "steps": [
                    {
                        "kind": "classify",
                        "runner": {"type": "echo", "outputs": {"labels": {"tagged": "yes"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"], "schema_version": "1.0"},
                        "apply": {"ops": [{"op": "set_tags", "from_output": "labels"}]},
                    }
                ],
                "feedback": {"work_results": []},
            }
        )
        result = kp.continue_run_work(first["flow_id"], first["requests"]["work"][0]["work_id"])
        second = kp.continue_flow(
            {
                "schema_version": "continue.v1",
                "request_id": "req-from-output-2",
                "flow_id": first["flow_id"],
                "state_version": first["state_version"],
                "feedback": {"work_results": [result]},
            }
        )
        assert second["status"] == "failed"
        assert second["errors"]
        assert second["errors"][0]["code"] == "invalid_input"
        assert "from_output is not supported" in second["errors"][0]["message"]
    finally:
        kp.close()
