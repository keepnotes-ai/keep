import json

import pytest

from keep.api import Keeper
from keep.api import _text_content_id


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


def _debug(payload: dict) -> dict:
    out = dict(payload)
    out["response_mode"] = "debug"
    return out


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
        assert len(first["work"]) == 1
        work_id = first["work"][0]["work_id"]

        work_result = kp.continue_run_work(first["cursor"], work_id)
        second = kp.continue_flow(
            {
                "request_id": "req-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
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
        assert "idempotency_key" not in first
        replay = kp.continue_flow(
            _summarize_request(
                "note:2",
                content,
                request_id="req-2",
                idempotency_key="idem-replay",
            )
        )
        assert "idempotency_key" not in replay
        assert replay["cursor"] == first["cursor"]
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
        _ = kp.continue_flow(
            {
                "request_id": "req-2a",
                "cursor": first["cursor"],
                "work_results": [],
            }
        )
        stale = kp.continue_flow(
            {
                "request_id": "req-2b",
                "cursor": first["cursor"],
                "work_results": [],
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
                "request_id": "req-frame-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:frame"},
                    "budget": {"max_nodes": 5},
                },
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        evidence = out["frame"]["evidence"]
        assert evidence
        assert evidence[0]["id"] == "note:frame"
    finally:
        kp.close()


def test_continue_program_persists_across_ticks(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="release plan and risks", id="note:persist", summary="release plan and risks")
        first = kp.continue_flow(
            _debug({
                "request_id": "req-persist-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "query", "value": "release plan"},
                    "budget": {"max_nodes": 5},
                },
                "work_results": [],
            })
        )
        second = kp.continue_flow(
            _debug({
                "request_id": "req-persist-2",
                "cursor": first["cursor"],
                "work_results": [],
            })
        )
        assert second["frame"]["debug"]["slots"]["goal"] == "query"
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
            _debug({
                "request_id": "req-auto-1",
                "goal": "query",
                "profile": "query.auto",
                "params": {"text": "alpha memory"},
                "frame_request": {
                    "seed": {"mode": "query", "value": "alpha memory"},
                    "pipeline": [{"op": "slice", "args": {"limit": 5}}],
                    "options": {"metadata": "basic"},
                },
                "work_results": [],
            })
        )
        assert first["status"] == "in_progress"
        assert (
            "auto_query_next_frame_request" in first["state"]["frontier"]
            or "auto_query_branch_plan" in first["state"]["frontier"]
        )

        current = first
        for i in range(1, 6):
            current = kp.continue_flow(
                _debug({
                    "request_id": f"req-auto-2-{i}",
                    "cursor": current["cursor"],
                    "work_results": [],
                })
            )
            if current["state"]["frontier"].get("auto_query_refined"):
                break
        assert current["status"] == "done"
        assert current["state"]["frontier"].get("auto_query_refined") is True
        assert "auto_query_next_frame_request" not in current["state"]["frontier"]
        assert current["state"]["program"]["frame_request"]["options"]["metadata"] == "basic"
    finally:
        kp.close()


def test_continue_query_auto_profile_single_lane_refine_adds_where(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="focus result one", id="note:auto-s1", tags={"conv": "7"}, summary="focus result one")
        kp.put(content="focus result two", id="note:auto-s2", tags={"conv": "7"}, summary="focus result two")
        kp.put(content="focus result other", id="note:auto-s3", tags={"conv": "3"}, summary="focus result other")

        first = kp.continue_flow(
            _debug({
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
                "work_results": [],
            })
        )
        assert first["status"] == "in_progress"
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


def test_continue_query_auto_profile_top2_plus_bridge_runs_branch_plan(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(
            content="bridge planning base one",
            id="note:auto-b1",
            tags={"project": "apollo", "topic": "auth", "speaker": "alice"},
            summary="bridge planning base one",
        )
        kp.put(
            content="bridge planning base two",
            id="note:auto-b2",
            tags={"project": "apollo", "topic": "auth", "speaker": "bob"},
            summary="bridge planning base two",
        )
        kp.put(
            content="bridge planning alt",
            id="note:auto-b3",
            tags={"project": "apollo", "topic": "billing", "speaker": "alice"},
            summary="bridge planning alt",
        )

        first = kp.continue_flow(
            _debug({
                "request_id": "req-auto-bridge-1",
                "goal": "query",
                "profile": "query.auto",
                "params": {"text": "bridge planning"},
                "frame_request": {
                    "seed": {"mode": "query", "value": "bridge planning"},
                    "pipeline": [{"op": "slice", "args": {"limit": 5}}],
                    "options": {"metadata": "basic"},
                },
                "decision_override": {
                    "strategy": "top2_plus_bridge",
                    "reason": "test_top2",
                },
                "work_results": [],
            })
        )
        assert first["status"] == "in_progress"
        plan = first["state"]["frontier"].get("auto_query_branch_plan")
        assert isinstance(plan, dict)
        pending = plan.get("pending")
        assert isinstance(pending, list)
        assert 1 <= len(pending) <= 3

        current = first
        for i in range(1, 6):
            current = kp.continue_flow(
                _debug({
                    "request_id": f"req-auto-bridge-next-{i}",
                    "cursor": current["cursor"],
                    "work_results": [],
                })
            )
            if current["state"]["frontier"].get("auto_query_refined"):
                break
        assert current["state"]["frontier"].get("auto_query_refined") is True
        selected = current["state"]["frontier"].get("auto_query_selected_branch")
        assert isinstance(selected, dict)
        assert selected.get("id")
    finally:
        kp.close()


def test_continue_invalid_frame_operator_fails(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        out = kp.continue_flow(
            {
                "request_id": "req-op-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "query", "value": "anything"},
                    "pipeline": [{"op": "explode", "args": {}}],
                },
                "work_results": [],
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
                "request_id": "req-meta-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:meta"},
                    "options": {"metadata": "basic"},
                },
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        evidence = out["frame"]["evidence"]
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
                "request_id": "req-meta-rich-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:meta-rich"},
                    "options": {"metadata": "rich"},
                },
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        evidence = out["frame"]["evidence"]
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
            _debug({
                "request_id": "req-decision-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "id", "value": "note:decision"},
                    "options": {"metadata": "basic"},
                },
                "work_results": [],
            })
        )
        assert out["status"] == "done"
        discriminators = out["frame"]["decision"]
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
        assert set(discriminators["lineage"].keys()) == {"version", "part"}
        assert set(discriminators["lineage"]["version"].keys()) == {
            "coverage_topk",
            "dominant_concentration_topk",
            "dominant",
            "distinct_topk",
        }
        assert set(discriminators["lineage"]["part"].keys()) == {
            "coverage_topk",
            "dominant_concentration_topk",
            "dominant",
            "distinct_topk",
        }
        assert set(discriminators["tag_profile"].keys()) == {
            "edge_key_count",
            "facet_key_count",
            "edge_keys",
            "facet_keys",
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
            _debug({
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
                "work_results": [],
            })
        )
        assert out["status"] == "done"
        policy_hint = out["frame"]["decision"]["policy_hint"]
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
                        "output_contract": {"must_return": ["labels"]},
                        "apply": {
                            "ops": [
                                {"op": "set_tags", "tags": "$output.labels"},
                            ]
                        },
                    }
                ],
                "work_results": [],
            }
        )
        assert first["status"] == "waiting_work"
        assert first["work"]
        assert first["work"][0]["kind"] == "classify"

        work_result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"],
        )
        second = kp.continue_flow(
            {
                "request_id": "req-classify-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
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
                "request_id": "req-quality-1",
                "goal": "pipeline",
                "params": {"id": "note:quality", "content": content},
                "steps": [
                    {
                        "kind": "quality_step",
                        "runner": {"type": "echo", "outputs": {"ok": True}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["ok"]},
                        "quality_gates": {"min_confidence": 0.7, "citation_required": True},
                        "escalate_if": ["low_confidence", "missing_citation"],
                    }
                ],
                "work_results": [],
            }
        )
        assert out["status"] == "waiting_work"
        assert out["work"]
        work = out["work"][0]
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
                        "output_contract": {"must_return": ["labels"]},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    }
                ],
                "work_results": [],
            }
        )
        work_result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"],
        )
        second = kp.continue_flow(
            {
                "request_id": "req-list-tags-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
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
                "request_id": "req-write-1",
                "goal": "write",
                "params": {"id": "note:write", "content": "hello", "tags": {"topic": "demo"}},
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        assert out["work"] == []
        item = kp.get("note:write")
        assert item is not None
        assert item.summary
        assert item.tags.get("topic") == "demo"
    finally:
        kp.close()


def test_continue_inline_write_without_id_uses_content_addressed_id(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "hello without explicit id"
        out = kp.continue_flow(
            {
                "request_id": "req-write-no-id-1",
                "goal": "write",
                "params": {"content": content, "tags": {"topic": "demo"}},
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        expected_id = _text_content_id(content)
        item = kp.get(expected_id)
        assert item is not None
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
                        "output_contract": {"must_return": ["labels"]},
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
                        "output_contract": {"must_return": ["summary"]},
                        "apply": {
                            "ops": [
                                {"op": "set_summary", "summary": "$output.summary"},
                            ]
                        },
                    },
                ],
                "work_results": [],
            }
        )
        assert first["status"] == "waiting_work"
        assert first["work"][0]["kind"] == "classify"

        classify_result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"],
        )
        second = kp.continue_flow(
            {
                "request_id": "req-order-2",
                "cursor": first["cursor"],
                "work_results": [classify_result],
            }
        )
        assert second["status"] == "done"
        tagged = kp.get("note:ordered")
        assert tagged is not None
        assert tagged.tags.get("doc_phase") == "classified"

        third = kp.continue_flow(
            {
                "request_id": "req-order-3",
                "cursor": second["cursor"],
                "work_results": [],
            }
        )
        assert third["status"] == "waiting_work"
        assert third["work"][0]["kind"] == "condense"
    finally:
        kp.close()


def test_continue_parallel_work_emission(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "parallel content"
        kp.put(content=content, id="note:parallel", summary=content)
        first = kp.continue_flow(
            {
                "request_id": "req-parallel-1",
                "goal": "pipeline",
                "params": {"id": "note:parallel", "content": content},
                "steps": [
                    {
                        "kind": "a",
                        "runner": {"type": "echo", "outputs": {"labels": {"a": "1"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"]},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    },
                    {
                        "kind": "b",
                        "runner": {"type": "echo", "outputs": {"labels": {"b": "1"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"]},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                    },
                ],
                "work_results": [],
            }
        )
        assert first["status"] == "waiting_work"
        kinds = sorted([w["kind"] for w in first["work"]])
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
                            "output_contract": {"must_return": ["labels"]},
                            "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels"}]},
                        }
                    ],
                },
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        assert out["work"] == []
    finally:
        kp.close()


def test_continue_rejects_unsupported_mutation_fields(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "unsupported mutation field example"
        kp.put(content=content, id="note:mut-op", summary=content)
        first = kp.continue_flow(
            {
                "request_id": "req-mut-op-1",
                "goal": "classify",
                "params": {"id": "note:mut-op", "content": content},
                "steps": [
                    {
                        "kind": "classify",
                        "runner": {"type": "echo", "outputs": {"labels": {"tagged": "yes"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"]},
                        "apply": {"ops": [{"op": "set_tags", "tags": "$output.labels", "unexpected": "x"}]},
                    }
                ],
                "work_results": [],
            }
        )
        result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"])
        second = kp.continue_flow(
            {
                "request_id": "req-mut-op-2",
                "cursor": first["cursor"],
                "work_results": [result],
            }
        )
        assert second["status"] == "failed"
        assert second["errors"]
        assert second["errors"][0]["code"] == "invalid_input"
        assert "Unsupported fields for set_tags" in second["errors"][0]["message"]
    finally:
        kp.close()


def test_continue_rejects_protected_system_mutation_targets(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "protected target"
        kp.put(content=content, id="note:protected-target", summary=content)
        first = kp.continue_flow(
            {
                "request_id": "req-protected-1",
                "goal": "classify",
                "params": {"id": "note:protected-target", "content": content},
                "steps": [
                    {
                        "kind": "classify",
                        "runner": {"type": "echo", "outputs": {"labels": {"tagged": "yes"}}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["labels"]},
                        "apply": {
                            "ops": [
                                {"op": "set_tags", "target": ".now", "tags": "$output.labels"},
                            ]
                        },
                    }
                ],
                "work_results": [],
            }
        )
        work_result = kp.continue_run_work(first["cursor"], first["work"][0]["work_id"])
        second = kp.continue_flow(
            {
                "request_id": "req-protected-2",
                "cursor": first["cursor"],
                "work_results": [work_result],
            }
        )
        assert second["status"] == "failed"
        assert second["errors"]
        assert second["errors"][0]["code"] == "forbidden_target"
    finally:
        kp.close()


def test_continue_executor_rejects_unallowed_runner_variable_refs(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "resolver allowlist"
        kp.put(content=content, id="note:resolver", summary=content)
        out = kp.continue_flow(
            {
                "request_id": "req-resolve-1",
                "goal": "pipeline",
                "params": {"id": "note:resolver", "content": content},
                "steps": [
                    {
                        "kind": "bad-ref",
                        "runner": {"type": "echo", "outputs": {"x": "$request_id"}},
                        "input_mode": "note_content",
                        "output_contract": {"must_return": ["x"]},
                    }
                ],
                "work_results": [],
            }
        )
        work_id = out["work"][0]["work_id"]
        with pytest.raises(ValueError, match="not allowed"):
            kp.continue_run_work(out["cursor"], work_id)
    finally:
        kp.close()


def test_continue_surfaces_frame_evidence_query_errors(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        original_find = kp.find

        def _raise_find(*args, **kwargs):
            raise RuntimeError("embedding down")

        kp.find = _raise_find  # type: ignore[assignment]
        try:
            out = kp.continue_flow(
                {
                    "request_id": "req-frame-error-1",
                    "goal": "query",
                    "frame_request": {
                        "seed": {"mode": "query", "value": "anything"},
                        "budget": {"max_nodes": 5},
                    },
                    "work_results": [],
                }
            )
            assert out["status"] == "failed"
            assert out["errors"]
            assert out["errors"][0]["code"] == "frame_evidence_error"
        finally:
            kp.find = original_find  # type: ignore[assignment]
    finally:
        kp.close()


def test_continuation_runtime_is_lazy_initialized(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    continuation_path = tmp_path / "continuation.db"
    try:
        assert not continuation_path.exists()
        out = kp.continue_flow(
            {
                "request_id": "req-lazy-1",
                "goal": "query",
                "frame_request": {
                    "seed": {"mode": "query", "value": "none"},
                },
                "work_results": [],
            }
        )
        assert out["status"] in {"done", "failed"}
        assert continuation_path.exists()
    finally:
        kp.close()


def test_put_routes_through_continuation_runtime(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    continuation_path = tmp_path / "continuation.db"
    try:
        assert not continuation_path.exists()
        item = kp.put(content="put via continuation", id="note:put-via-cont")
        assert item.id == "note:put-via-cont"
        assert continuation_path.exists()
    finally:
        kp.close()


def test_put_long_content_schedules_continuation_work(monkeypatch, mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        monkeypatch.setattr(kp, "_spawn_processor", lambda: False)
        monkeypatch.setattr(kp, "_needs_sysdoc_migration", False)
        baseline_pending = kp.pending_count()
        baseline_continuation = kp.continuation_pending_count()
        content = "default queue " * 200
        item = kp.put(content=content, id="note:default-queue")
        assert item.summary.endswith("...")
        assert kp.pending_count() == baseline_pending
        # summarize + analyze + tag all fire for non-system items
        assert kp.continuation_pending_count() == baseline_continuation + 3
    finally:
        kp.close()


def test_put_long_content_continuation_work_processing_updates_summary(monkeypatch, mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        monkeypatch.setattr(kp, "_spawn_processor", lambda: False)
        monkeypatch.setattr(kp, "_needs_sysdoc_migration", False)
        baseline_pending = kp.pending_count()
        baseline_continuation = kp.continuation_pending_count()
        content = "continuation queue " * 200
        item = kp.put(content=content, id="note:continuation-queue")
        assert item.summary.endswith("...")
        assert kp.pending_count() == baseline_pending
        # summarize + analyze + tag all fire for non-system items
        assert kp.continuation_pending_count() == baseline_continuation + 3

        result = kp.process_continuation_work(limit=10, worker_id="test-worker")
        assert result["claimed"] >= 1
        assert result["processed"] >= 1
        assert result["failed"] == 0
        assert result["dead_lettered"] == 0

        updated = kp.get("note:continuation-queue")
        assert updated is not None
        assert updated.summary == content[:200]
        assert kp.continuation_pending_count() == 0
    finally:
        kp.close()


def test_enqueue_analyze_schedules_continuation_work(monkeypatch, mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        monkeypatch.setattr(kp, "_spawn_processor", lambda: False)
        monkeypatch.setattr(kp, "_needs_sysdoc_migration", False)
        kp.put(content="Analyze me " * 60, id="note:analyze-queued")
        baseline = kp.continuation_pending_count()
        queued = kp.enqueue_analyze("note:analyze-queued")
        assert queued is True
        assert kp.continuation_pending_count() == baseline + 1
    finally:
        kp.close()


def test_enqueue_analyze_continuation_work_executes_local_task(monkeypatch, mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    calls: list[dict] = []
    original = kp._run_local_task_workflow
    try:
        monkeypatch.setattr(kp, "_spawn_processor", lambda: False)
        monkeypatch.setattr(kp, "_needs_sysdoc_migration", False)
        kp.put(content="Analyze me " * 60, id="note:analyze-run")

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
            return {"status": "applied", "details": {"parts_count": 2}}

        kp._run_local_task_workflow = _fake_run_local_task_workflow  # type: ignore[assignment]
        queued = kp.enqueue_analyze("note:analyze-run", tags=["topic"], force=True)
        assert queued is True
        result = kp.process_continuation_work(limit=10, worker_id="test-worker")
        assert result["processed"] >= 1
        assert any(call["task_type"] == "analyze" for call in calls)
    finally:
        kp._run_local_task_workflow = original  # type: ignore[assignment]
        kp.close()


def test_enqueue_ocr_background_continuation_executes_local_task(monkeypatch, mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    calls: list[dict] = []
    original = kp._run_local_task_workflow
    try:
        monkeypatch.setattr(kp, "_spawn_processor", lambda: False)
        monkeypatch.setattr(kp, "_needs_sysdoc_migration", False)

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
            return {"status": "applied", "details": {"chars": 42}}

        kp._run_local_task_workflow = _fake_run_local_task_workflow  # type: ignore[assignment]
        doc_coll = kp._resolve_doc_collection()
        kp._enqueue_ocr_background(
            id="note:ocr-run",
            doc_coll=doc_coll,
            uri="file:///tmp/scan.pdf",
            ocr_pages=[1, 3],
            content_type="application/pdf",
        )
        result = kp.process_continuation_work(limit=10, worker_id="test-worker")
        assert result["processed"] >= 1
        assert any(call["task_type"] == "ocr" for call in calls)
    finally:
        kp._run_local_task_workflow = original  # type: ignore[assignment]
        kp.close()


def test_continue_rejects_oversized_payload(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        huge = "x" * 600_000
        with pytest.raises(ValueError, match="exceeds max size"):
            kp.continue_flow(
                {
                    "request_id": "req-too-big-1",
                    "goal": "write",
                    "params": {"id": "note:big", "content": huge},
                    "work_results": [],
                }
            )
    finally:
        kp.close()


def test_continue_replays_pending_mutations_on_tick(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="pending mutation source", id="note:pending", summary="pending mutation source")
        runtime = kp._get_continuation_runtime()
        mutation_id = runtime._insert_pending_mutation(
            flow_id="f_test_pending",
            work_id=None,
            op={"op": "set_tags", "target": "note:pending", "tags": {"replayed": "yes"}},
        )
        row = runtime._get_mutation(mutation_id)
        assert row is not None and row.status == "pending"

        out = kp.continue_flow(
            {
                "request_id": "req-replay-1",
                "goal": "query",
                "frame_request": {"seed": {"mode": "id", "value": "note:pending"}},
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        item = kp.get("note:pending")
        assert item is not None
        assert item.tags.get("replayed") == "yes"
        status_row = runtime._get_mutation(mutation_id)
        assert status_row is not None and status_row.status == "applied"
    finally:
        kp.close()


def test_continue_mutation_journal_is_idempotent_for_duplicate_ops(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="dup mutation source", id="note:dup-mutation", summary="dup mutation source")
        runtime = kp._get_continuation_runtime()
        op = {"op": "set_tags", "target": "note:dup-mutation", "tags": {"k": "v"}}
        first = runtime._insert_pending_mutation(flow_id="f_dup", work_id="w_dup", op=op)
        second = runtime._insert_pending_mutation(flow_id="f_dup", work_id="w_dup", op=op)
        assert first == second
        rows = runtime._list_pending_mutations(flow_id="f_dup", limit=20)
        assert len([row for row in rows if row.mutation_id == first]) == 1
    finally:
        kp.close()


def test_continue_work_result_mutations_are_queued_then_replayed(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        content = "queued replay content"
        kp.put(content=content, id="note:queued", summary="placeholder")
        first = kp.continue_flow(
            _summarize_request("note:queued", content, request_id="req-queued-1")
        )
        work_id = first["work"][0]["work_id"]
        wr = kp.continue_run_work(first["cursor"], work_id)
        second = kp.continue_flow(
            {
                "request_id": "req-queued-2",
                "cursor": first["cursor"],
                "work_results": [wr],
            }
        )
        assert second["status"] == "done"
        work_ops = second["applied_ops"]
        assert work_ops
        op_entries = [op for op in work_ops if op.get("source") == "work" and op.get("op") == "set_summary"]
        assert op_entries
        assert op_entries[0]["status"] in {"queued", "applied"}
        runtime = kp._get_continuation_runtime()
        mutation_id = op_entries[0].get("mutation_id")
        assert isinstance(mutation_id, str) and mutation_id
        row = runtime._get_mutation(mutation_id)
        assert row is not None
        assert row.status == "applied"
        item = kp.get("note:queued")
        assert item is not None
        assert item.summary == content[:200]
    finally:
        kp.close()


def test_continue_default_response_omits_state_and_output_hash(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="standard response", id="note:resp-std", summary="standard response")
        out = kp.continue_flow(
            {
                "request_id": "req-resp-std-1",
                "goal": "query",
                "frame_request": {"seed": {"mode": "id", "value": "note:resp-std"}},
                "work_results": [],
            }
        )
        assert out["status"] == "done"
        assert "state" not in out
        assert "output_hash" not in out
    finally:
        kp.close()


def test_continue_debug_response_includes_state_and_output_hash(mock_providers, tmp_path):
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put(content="debug response", id="note:resp-debug", summary="debug response")
        out = kp.continue_flow(
            _debug(
                {
                    "request_id": "req-resp-debug-1",
                    "goal": "query",
                    "frame_request": {"seed": {"mode": "id", "value": "note:resp-debug"}},
                    "work_results": [],
                }
            )
        )
        assert out["status"] == "done"
        assert isinstance(out.get("state"), dict)
        assert isinstance(out.get("output_hash"), str) and out["output_hash"]
    finally:
        kp.close()


