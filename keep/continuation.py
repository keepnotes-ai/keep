"""
Local continuation runtime (API-first, local-only scope).

Implements a minimal `continue(input) -> output` loop with durable flow state
and idempotency.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from .continuation_executor import LocalWorkExecutor

if TYPE_CHECKING:
    from .api import Keeper

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "continue.v1"
ALLOWED_FRAME_OPS = {"where", "slice"}
DECISION_SUPPORT_VERSION = "ds.v1"
DECISION_STRATEGIES = {"single_lane_refine", "top2_plus_bridge", "explore_more"}
BUILTIN_QUERY_AUTO_PROFILES = {"query.auto", ".profile/query.auto"}
NON_PIVOT_TAG_KEYS = {"type", "context", "category"}
DEFAULT_DECISION_POLICY = {
    "margin_high": 0.18,
    "entropy_low": 0.45,
    "margin_low": 0.08,
    "entropy_high": 0.72,
    "lineage_strong": 0.75,
    "pivot_topk": 6,
    "max_pivots": 2,
}
_BIND_EXPR_RE = re.compile(r"\$\{(params|slots)\.([A-Za-z0-9_]+)(?:\|([^}]*))?\}")


@dataclass
class _FlowRow:
    flow_id: str
    state_version: int
    status: str
    state_json: str


@dataclass
class _WorkRow:
    work_id: str
    flow_id: str
    kind: str
    status: str
    input_json: str
    output_contract_json: str
    attempt: int


class LocalContinuationRuntime:
    """Durable local continuation runtime."""

    def __init__(self, db_path: Path, keeper: "Keeper") -> None:
        self._db_path = db_path
        self._keeper = keeper
        self._work_executor = LocalWorkExecutor(keeper)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False, isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS continue_flows (
                flow_id TEXT PRIMARY KEY,
                state_version INTEGER NOT NULL,
                status TEXT NOT NULL,
                state_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS continue_work (
                work_id TEXT PRIMARY KEY,
                flow_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                input_json TEXT NOT NULL,
                output_contract_json TEXT NOT NULL,
                result_json TEXT,
                attempt INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_continue_work_flow_status
            ON continue_work(flow_id, status)
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS continue_idempotency (
                idempotency_key TEXT PRIMARY KEY,
                request_hash TEXT NOT NULL,
                output_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS continue_events (
                event_id TEXT PRIMARY KEY,
                flow_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _hash_json(payload: dict[str, Any]) -> str:
        material = dict(payload)
        # request_id is per-call and should not affect idempotent replay keys.
        material.pop("request_id", None)
        canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _output_hash(payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _initial_state() -> dict[str, Any]:
        return {
            "cursor": {"step": 0},
            "frontier": {},
            "program": {},
            "pending": {"work_ids": []},
            "budget_used": {"tokens": 0, "nodes": 0},
            "termination": {
                "idle_ticks": 0,
                "max_idle_ticks": 3,
            },
        }

    def _get_flow(self, flow_id: str) -> Optional[_FlowRow]:
        row = self._conn.execute(
            "SELECT flow_id, state_version, status, state_json FROM continue_flows WHERE flow_id = ?",
            (flow_id,),
        ).fetchone()
        if row is None:
            return None
        return _FlowRow(
            flow_id=row["flow_id"],
            state_version=int(row["state_version"]),
            status=row["status"],
            state_json=row["state_json"],
        )

    def _create_flow(self) -> _FlowRow:
        flow_id = f"f_{uuid.uuid4().hex[:10]}"
        now = self._now()
        state_json = json.dumps(self._initial_state(), ensure_ascii=False)
        self._conn.execute(
            """
            INSERT INTO continue_flows(flow_id, state_version, status, state_json, created_at, updated_at)
            VALUES (?, 0, 'running', ?, ?, ?)
            """,
            (flow_id, state_json, now, now),
        )
        return _FlowRow(flow_id=flow_id, state_version=0, status="running", state_json=state_json)

    def _load_idempotent(self, key: str) -> Optional[tuple[str, str]]:
        row = self._conn.execute(
            "SELECT request_hash, output_json FROM continue_idempotency WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return str(row["request_hash"]), str(row["output_json"])

    def _store_idempotent(self, key: str, request_hash: str, output: dict[str, Any]) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO continue_idempotency(idempotency_key, request_hash, output_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (key, request_hash, json.dumps(output, ensure_ascii=False), self._now()),
        )

    def _list_requested_work(self, flow_id: str) -> list[_WorkRow]:
        rows = self._conn.execute(
            """
            SELECT work_id, flow_id, kind, status, input_json, output_contract_json, attempt
            FROM continue_work
            WHERE flow_id = ? AND status = 'requested'
            ORDER BY created_at ASC
            """,
            (flow_id,),
        ).fetchall()
        return [
            _WorkRow(
                work_id=row["work_id"],
                flow_id=row["flow_id"],
                kind=row["kind"],
                status=row["status"],
                input_json=row["input_json"],
                output_contract_json=row["output_contract_json"],
                attempt=int(row["attempt"]),
            )
            for row in rows
        ]

    def _get_work(self, flow_id: str, work_id: str) -> Optional[_WorkRow]:
        row = self._conn.execute(
            """
            SELECT work_id, flow_id, kind, status, input_json, output_contract_json, attempt
            FROM continue_work
            WHERE flow_id = ? AND work_id = ?
            """,
            (flow_id, work_id),
        ).fetchone()
        if row is None:
            return None
        return _WorkRow(
            work_id=row["work_id"],
            flow_id=row["flow_id"],
            kind=row["kind"],
            status=row["status"],
            input_json=row["input_json"],
            output_contract_json=row["output_contract_json"],
            attempt=int(row["attempt"]),
        )

    def _infer_note_id(self, program: dict[str, Any]) -> Optional[str]:
        params = program.get("params") or {}
        if isinstance(params, dict) and params.get("id"):
            return str(params["id"])

        frame_req = program.get("frame_request") or {}
        if isinstance(frame_req, dict):
            seed = frame_req.get("seed") or {}
            if isinstance(seed, dict) and seed.get("mode") == "id" and seed.get("value"):
                return str(seed["value"])
        return None

    def _goal_from_program(self, program: dict[str, Any]) -> str:
        goal = str(program.get("goal") or "").strip().lower()
        return goal

    @staticmethod
    def _profile_from_program(program: dict[str, Any]) -> str:
        return str(program.get("profile") or "").strip()

    def _is_query_auto_profile(self, program: dict[str, Any]) -> bool:
        return self._profile_from_program(program) in BUILTIN_QUERY_AUTO_PROFILES

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _program_has_inputs(payload: dict[str, Any]) -> bool:
        return any(
            key in payload
            for key in (
                "goal",
                "profile",
                "success_tests",
                "template_ref",
                "params",
                "frame_request",
                "decision_policy",
                "steps",
                "stages",
            )
        )

    @staticmethod
    def _legacy_input_fields(payload: dict[str, Any]) -> list[str]:
        legacy = []
        for key in ("intent", "work_plan", "write"):
            if key in payload:
                legacy.append(key)
        return legacy

    def _merge_program(self, state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        existing = state.get("program") or {}
        if not isinstance(existing, dict):
            existing = {}

        if not self._program_has_inputs(payload):
            return existing

        merged = dict(existing)
        if "goal" in payload:
            merged["goal"] = str(payload.get("goal") or "").strip().lower()
        if "profile" in payload:
            merged["profile"] = str(payload.get("profile") or "").strip()
        if "success_tests" in payload:
            tests = payload.get("success_tests")
            if isinstance(tests, list):
                merged["success_tests"] = [str(x) for x in tests if str(x).strip()]
            else:
                merged["success_tests"] = []
        if "template_ref" in payload:
            merged["template_ref"] = str(payload.get("template_ref") or "").strip()
        if "params" in payload:
            params = payload.get("params")
            merged["params"] = params if isinstance(params, dict) else {}
        if "frame_request" in payload:
            frame_request = payload.get("frame_request")
            merged["frame_request"] = frame_request if isinstance(frame_request, dict) else {}
        if "decision_policy" in payload:
            policy = payload.get("decision_policy")
            merged["decision_policy"] = policy if isinstance(policy, dict) else {}
        if "steps" in payload:
            steps = payload.get("steps")
            merged["steps"] = [dict(step) for step in steps if isinstance(step, dict)] if isinstance(steps, list) else []
        if "stages" in payload:
            merged["stages"] = self._normalize_profile_stages(payload.get("stages"))
        merged["goal"] = self._goal_from_program(merged)
        return merged

    def _effective_frame_request(self, program: dict[str, Any]) -> dict[str, Any]:
        frame_request = program.get("frame_request")
        if isinstance(frame_request, dict):
            return frame_request

        template_request = program.get("_template_frame_request")
        if isinstance(template_request, dict):
            return template_request

        params = program.get("params") or {}
        if isinstance(params, dict) and params.get("text"):
            return {
                "seed": {"mode": "query", "value": str(params["text"])},
                "budget": {"max_nodes": 10},
            }

        note_id = self._infer_note_id(program)
        if note_id:
            return {
                "seed": {"mode": "id", "value": note_id},
                "budget": {"max_nodes": 1},
            }
        return {}

    def _validate_frame_request(self, frame_request: dict[str, Any]) -> Optional[dict[str, str]]:
        pipeline = frame_request.get("pipeline") or []
        if not isinstance(pipeline, list):
            return {
                "code": "invalid_input",
                "message": "frame_request.pipeline must be a list",
            }
        for stage in pipeline:
            if not isinstance(stage, dict):
                return {
                    "code": "invalid_input",
                    "message": "frame_request.pipeline entries must be objects",
                }
            op = str(stage.get("op") or "")
            if op and op not in ALLOWED_FRAME_OPS:
                return {
                    "code": "invalid_frame_operator",
                    "message": f"Unsupported frame operator: {op}",
                }
        return None

    @staticmethod
    def _parse_where_tags(frame_request: dict[str, Any]) -> dict[str, Any]:
        tags: dict[str, Any] = {}
        pipeline = frame_request.get("pipeline") or []
        for stage in pipeline:
            if not isinstance(stage, dict):
                continue
            if str(stage.get("op") or "") != "where":
                continue
            args = stage.get("args") or {}
            if not isinstance(args, dict):
                continue
            facts = args.get("facts") or []
            if not isinstance(facts, list):
                continue
            for fact in facts:
                if not isinstance(fact, str):
                    continue
                if "=" not in fact:
                    continue
                key, value = fact.split("=", 1)
                key = key.strip().casefold()
                value = value.strip()
                if not key or not value:
                    continue
                current = tags.get(key)
                if current is None:
                    tags[key] = value
                elif isinstance(current, list):
                    if value not in current:
                        current.append(value)
                elif current != value:
                    tags[key] = [str(current), value]
        return tags

    def _pipeline_limit(self, frame_request: dict[str, Any], default_limit: int) -> int:
        limit = default_limit
        pipeline = frame_request.get("pipeline") or []
        for stage in pipeline:
            if not isinstance(stage, dict):
                continue
            if str(stage.get("op") or "") != "slice":
                continue
            args = stage.get("args") or {}
            if not isinstance(args, dict):
                continue
            limit = self._as_int(args.get("limit"), limit)
        return limit

    @staticmethod
    def _normalize_metadata_level(value: Any) -> str:
        level = str(value or "basic").strip().lower()
        if level not in {"none", "basic", "rich"}:
            return "basic"
        return level

    @staticmethod
    def _user_tags(tags: dict[str, Any], *, max_keys: int = 24) -> dict[str, Any]:
        if not isinstance(tags, dict):
            return {}
        out: dict[str, Any] = {}
        for key in sorted(tags.keys()):
            if str(key).startswith("_"):
                continue
            out[str(key)] = tags[key]
            if len(out) >= max_keys:
                break
        return out

    def _evidence_item(self, item: Any, role: str, *, metadata_level: str) -> dict[str, Any]:
        tags = item.tags if isinstance(getattr(item, "tags", None), dict) else {}
        base_id = str(tags.get("_base_id") or item.id)
        user_tags = self._user_tags(tags)
        metadata: dict[str, Any] = {
            "level": metadata_level,
            "base_id": base_id,
            "source": tags.get("_source"),
            "created": tags.get("_created"),
            "updated": tags.get("_updated"),
            "accessed": tags.get("_accessed"),
            "total_parts": tags.get("_total_parts"),
            "focus": {
                "version": tags.get("_focus_version"),
                "part": tags.get("_focus_part"),
            },
            "tags": user_tags,
        }
        if metadata_level == "rich":
            try:
                ctx = self._keeper.get_context(
                    base_id,
                    include_similar=True,
                    include_meta=True,
                    include_parts=True,
                    include_versions=True,
                )
                if ctx is not None:
                    metadata["links"] = {
                        "similar": [
                            {"id": s.id, "score": s.score, "date": s.date}
                            for s in ctx.similar[:5]
                        ],
                        "meta_sections": sorted(ctx.meta.keys()),
                        "edges": {
                            k: [ref.source_id for ref in refs[:8]]
                            for k, refs in ctx.edges.items()
                        },
                    }
                    metadata["structure"] = {
                        "parts": len(ctx.parts),
                        "prev_versions": len(ctx.prev),
                        "next_versions": len(ctx.next),
                    }
            except Exception as exc:
                metadata["metadata_error"] = str(exc)
        return {
            "id": item.id,
            "role": role,
            "score": item.score,
            "summary": item.summary,
            "metadata": {} if metadata_level == "none" else metadata,
        }

    def _frame_evidence_for_request(
        self, frame_request: dict[str, Any],
    ) -> list[dict[str, Any]]:
        seed = frame_request.get("seed") or {}
        if not isinstance(seed, dict):
            return []

        mode = str(seed.get("mode") or "")
        value = seed.get("value")
        budget = frame_request.get("budget") or {}
        options = frame_request.get("options") or {}
        if not isinstance(options, dict):
            options = {}
        deep = bool(options.get("deep"))
        metadata_level = self._normalize_metadata_level(options.get("metadata"))
        where_tags = self._parse_where_tags(frame_request)
        max_nodes = self._as_int(
            budget.get("max_nodes") if isinstance(budget, dict) else None,
            10,
        )
        limit = self._pipeline_limit(frame_request, default_limit=max_nodes)
        limit = min(max(limit, 1), 100)

        if mode == "id" and value:
            item = self._keeper.get(str(value))
            if item is None:
                return []
            return [self._evidence_item(item, role="target", metadata_level=metadata_level)]

        if mode == "query" and value:
            try:
                items = self._keeper.find(
                    query=str(value),
                    tags=where_tags or None,
                    limit=limit,
                    deep=deep,
                )
            except Exception:
                return []
            return [
                self._evidence_item(it, role="candidate", metadata_level=metadata_level)
                for it in items[:limit]
            ]

        if mode == "similar_to" and value:
            try:
                items = self._keeper.find(
                    similar_to=str(value),
                    tags=where_tags or None,
                    limit=limit,
                    deep=deep,
                )
            except Exception:
                return []
            return [
                self._evidence_item(it, role="neighbor", metadata_level=metadata_level)
                for it in items[:limit]
            ]
        return []

    def _frame_evidence(self, program: dict[str, Any]) -> list[dict[str, Any]]:
        frame_request = self._effective_frame_request(program)
        return self._frame_evidence_for_request(frame_request)

    def _content_for_work(
        self, note_id: str, program: dict[str, Any], doc_coll: str,
    ) -> tuple[Optional[str], Optional[dict[str, str]]]:
        doc = self._keeper._document_store.get(doc_coll, note_id)
        if doc is None:
            return None, {
                "code": "missing_required_field",
                "message": "Work flow requires existing note id",
            }

        params = program.get("params") or {}
        content = None
        if isinstance(params, dict) and params.get("content"):
            content = str(params["content"])
        if not content:
            content = doc.summary or ""
        if not content:
            return None, {
                "code": "missing_required_field",
                "message": "Work flow requires params.content or existing note content",
            }
        return content, None

    def _program_steps(self, program: dict[str, Any]) -> list[dict[str, Any]]:
        plan = program.get("steps")
        if isinstance(plan, list):
            return [step for step in plan if isinstance(step, dict)]
        return []

    @staticmethod
    def _normalize_profile_stages(stages: Any) -> list[dict[str, Any]]:
        if not isinstance(stages, list):
            return []
        normalized: list[dict[str, Any]] = []
        for idx, raw in enumerate(stages):
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or raw.get("emits_work") or f"stage_{idx}").strip()
            if not name:
                continue
            stage: dict[str, Any] = {
                "name": name,
                "when": raw.get("when"),
                "terminal": bool(raw.get("terminal")),
            }
            emits_work = raw.get("emits_work")
            if isinstance(emits_work, str) and emits_work.strip():
                stage["emits_work"] = [emits_work.strip()]
            elif isinstance(emits_work, list):
                stage["emits_work"] = [str(k).strip() for k in emits_work if str(k).strip()]
            for key in (
                "runner",
                "input_mode",
                "input",
                "output_contract",
                "apply",
                "executor",
                "executor_class",
                "executor_id",
                "quality_gates",
                "escalate_if",
            ):
                if key in raw:
                    stage[key] = raw[key]
            normalized.append(stage)
        return normalized

    def _profile_plan_and_stages(
        self, program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Optional[dict[str, str]]]:
        if self._is_query_auto_profile(program):
            return [], [], None

        profile = str(program.get("profile") or "").strip()
        if not profile:
            return [], [], None

        profile_id = profile if profile.startswith(".profile/") else f".profile/{profile}"
        doc = self._keeper.get(profile_id)
        if doc is None:
            return [], [], {"code": "unknown_template", "message": f"Profile not found: {profile_id}"}

        raw = str(doc.summary or "").strip()
        if not raw:
            return [], [], {"code": "invalid_input", "message": f"Profile is empty: {profile_id}"}

        parsed: Any = None
        try:
            parsed = json.loads(raw)
        except Exception:
            try:
                import yaml

                parsed = yaml.safe_load(raw)
            except Exception:
                parsed = None
        if not isinstance(parsed, dict):
            return [], [], {"code": "invalid_input", "message": f"Profile must parse to an object: {profile_id}"}

        if not isinstance(program.get("decision_policy"), dict):
            profile_policy = parsed.get("decision_policy")
            if isinstance(profile_policy, dict):
                program["decision_policy"] = dict(profile_policy)

        steps = parsed.get("steps")
        if isinstance(steps, list):
            normalized_steps = [dict(step) for step in steps if isinstance(step, dict)]
            return normalized_steps, [], None

        stages = self._normalize_profile_stages(parsed.get("stages"))
        if not stages:
            return [], [], {"code": "invalid_input", "message": f"Profile missing steps/stages: {profile_id}"}

        compiled: list[dict[str, Any]] = []
        for stage in stages:
            kinds = stage.get("emits_work") or []
            if not isinstance(kinds, list):
                kinds = []
            for kind in kinds:
                step: dict[str, Any] = {"kind": str(kind), "stage": stage.get("name")}
                when = stage.get("when")
                if when is not None:
                    step["when"] = when
                for key in (
                    "runner",
                    "input_mode",
                    "input",
                    "output_contract",
                    "apply",
                    "executor",
                    "executor_class",
                    "executor_id",
                    "quality_gates",
                    "escalate_if",
                ):
                    if key in stage:
                        step[key] = stage[key]
                compiled.append(step)
        return compiled, stages, None

    def _resolved_plan_and_stages(
        self, program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Optional[dict[str, str]]]:
        inline = self._program_steps(program)
        if inline:
            inline_stages = self._normalize_profile_stages(program.get("stages"))
            return inline, inline_stages, None
        return self._profile_plan_and_stages(program)

    def _active_process_stage(
        self,
        *,
        flow_id: str,
        program: dict[str, Any],
        state: dict[str, Any],
        stages: list[dict[str, Any]],
    ) -> Optional[str]:
        if not stages:
            return None
        stage_names = [str(s.get("name") or "") for s in stages]
        cursor = state.get("cursor") or {}
        stage = str(cursor.get("stage") or "")
        if stage in stage_names and self._when_matches(
            stages[stage_names.index(stage)].get("when"),
            flow_id=flow_id,
            program=program,
        ):
            return stage
        for candidate in stages:
            if self._when_matches(candidate.get("when"), flow_id=flow_id, program=program):
                return str(candidate.get("name") or "")
        return None

    def _stage_step_keys(
        self,
        *,
        flow_id: str,
        program: dict[str, Any],
        stage_name: str,
        plan: list[dict[str, Any]],
    ) -> list[str]:
        keys: list[str] = []
        for idx, step in enumerate(plan):
            if str(step.get("stage") or "") != stage_name:
                continue
            if not self._when_matches(step.get("when"), flow_id=flow_id, program=program):
                continue
            keys.append(self._work_step_key(step, idx))
        return keys

    def _next_process_stage(
        self,
        *,
        flow_id: str,
        program: dict[str, Any],
        current_stage: str,
        stages: list[dict[str, Any]],
    ) -> Optional[str]:
        stage_names = [str(s.get("name") or "") for s in stages]
        if current_stage not in stage_names:
            return None
        for candidate in stages[stage_names.index(current_stage) + 1:]:
            if self._when_matches(candidate.get("when"), flow_id=flow_id, program=program):
                return str(candidate.get("name") or "")
        return None

    @staticmethod
    def _work_step_key(step: dict[str, Any], idx: int) -> str:
        kind = str(step.get("kind") or "").strip()
        if kind:
            return kind
        return f"step_{idx}"

    def _has_any_work_key(self, flow_id: str, key: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM continue_work
            WHERE flow_id = ? AND kind = ?
            LIMIT 1
            """,
            (flow_id, key),
        ).fetchone()
        return row is not None

    def _has_completed_work_key(self, flow_id: str, key: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM continue_work
            WHERE flow_id = ? AND kind = ? AND status = 'completed'
            LIMIT 1
            """,
            (flow_id, key),
        ).fetchone()
        return row is not None

    def _has_finished_work_key(self, flow_id: str, key: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM continue_work
            WHERE flow_id = ? AND kind = ? AND status IN ('completed', 'failed')
            LIMIT 1
            """,
            (flow_id, key),
        ).fetchone()
        return row is not None

    @staticmethod
    def _param_value(program: dict[str, Any], key: str) -> Any:
        params = program.get("params") or {}
        if not isinstance(params, dict):
            return None
        if "." not in key:
            return params.get(key)
        value: Any = params
        for part in key.split("."):
            if not isinstance(value, dict):
                return None
            value = value.get(part)
        return value

    def _when_matches(self, when: Any, *, flow_id: str, program: dict[str, Any]) -> bool:
        if when is None:
            return True
        if isinstance(when, bool):
            return bool(when)
        if isinstance(when, str):
            value = self._param_value(program, when)
            return bool(value)
        if not isinstance(when, dict):
            return False

        if "all" in when:
            checks = when.get("all")
            if not isinstance(checks, list):
                return False
            return all(self._when_matches(check, flow_id=flow_id, program=program) for check in checks)
        if "any" in when:
            checks = when.get("any")
            if not isinstance(checks, list):
                return False
            return any(self._when_matches(check, flow_id=flow_id, program=program) for check in checks)
        if "not" in when:
            return not self._when_matches(when.get("not"), flow_id=flow_id, program=program)
        if "param_true" in when:
            return bool(self._param_value(program, str(when.get("param_true"))))
        if "param_equals" in when:
            cond = when.get("param_equals")
            if not isinstance(cond, dict):
                return False
            key = str(cond.get("key") or "")
            return self._param_value(program, key) == cond.get("value")
        if "work_completed" in when:
            return self._has_completed_work_key(flow_id, str(when.get("work_completed") or ""))
        if "work_not_completed" in when:
            return not self._has_completed_work_key(flow_id, str(when.get("work_not_completed") or ""))
        return False

    def _insert_work(
        self,
        *,
        flow_id: str,
        kind: str,
        executor_class: str,
        input_payload: dict[str, Any],
        output_contract: dict[str, Any],
        quality_gates: dict[str, Any],
        escalate_if: list[str],
        executor_id: str,
    ) -> dict[str, Any]:
        work_id = f"w_{uuid.uuid4().hex[:10]}"
        now = self._now()
        self._conn.execute(
            """
            INSERT INTO continue_work(
                work_id, flow_id, kind, status, input_json, output_contract_json,
                result_json, attempt, created_at, updated_at
            ) VALUES (?, ?, ?, 'requested', ?, ?, NULL, 1, ?, ?)
            """,
            (
                work_id,
                flow_id,
                kind,
                json.dumps(input_payload, ensure_ascii=False),
                json.dumps(output_contract, ensure_ascii=False),
                now,
                now,
            ),
        )
        return {
            "work_id": work_id,
            "kind": kind,
            "executor_class": executor_class or "unspecified",
            "suggested_executor_id": executor_id,
            "input": {"id": input_payload.get("item_id")},
            "output_contract": output_contract,
            "quality_gates": quality_gates,
            "escalate_if": escalate_if,
        }

    def _enqueue_work_step(
        self,
        *,
        flow_id: str,
        program: dict[str, Any],
        step: dict[str, Any],
        step_idx: int,
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, str]]]:
        kind = self._work_step_key(step, step_idx)
        input_mode = str(step.get("input_mode") or "note_content")
        output_contract = step.get("output_contract") or {"schema_version": "1.0"}
        if not isinstance(output_contract, dict):
            return None, {
                "code": "invalid_input",
                "message": "work step output_contract must be an object",
            }
        runner = step.get("runner") or {}
        if not isinstance(runner, dict):
            return None, {"code": "invalid_input", "message": "work step runner must be an object"}

        executor = step.get("executor") or {}
        if not isinstance(executor, dict):
            executor = {}
        executor_class = str(step.get("executor_class") or executor.get("class") or "unspecified")
        executor_id = str(step.get("executor_id") or executor.get("id") or "")
        quality_gates = step.get("quality_gates") or {}
        if not isinstance(quality_gates, dict):
            return None, {"code": "invalid_input", "message": "work step quality_gates must be an object"}
        if not quality_gates:
            quality_gates = {
                "min_confidence": 0.0,
                "citation_required": False,
            }
        escalate_if = step.get("escalate_if") or []
        if not isinstance(escalate_if, list):
            return None, {"code": "invalid_input", "message": "work step escalate_if must be a list"}
        normalized_escalate_if = [str(reason) for reason in escalate_if if str(reason).strip()]

        input_payload: dict[str, Any] = {
            "runner": runner,
            "apply": step.get("apply") or {},
            "_executor_class": executor_class,
            "_executor_id": executor_id,
            "_quality_gates": quality_gates,
            "_escalate_if": normalized_escalate_if,
        }

        if input_mode == "note_content":
            note_id = self._infer_note_id(program)
            if not note_id:
                return None, {
                    "code": "missing_required_field",
                    "message": "work step with input_mode=note_content requires note id",
                }
            doc_coll = self._keeper._resolve_doc_collection()
            content, err = self._content_for_work(note_id, program, doc_coll)
            if err is not None:
                return None, err
            input_payload.update({
                "item_id": note_id,
                "collection": doc_coll,
                "content": content,
            })

        extra_input = step.get("input") or {}
        if extra_input:
            if not isinstance(extra_input, dict):
                return None, {"code": "invalid_input", "message": "work step input must be an object"}
            input_payload.update(extra_input)

        request_work = self._insert_work(
            flow_id=flow_id,
            kind=kind,
            executor_class=executor_class,
            input_payload=input_payload,
            output_contract=output_contract,
            quality_gates=quality_gates,
            escalate_if=normalized_escalate_if,
            executor_id=executor_id,
        )
        return request_work, None

    def _plan_work(
        self,
        flow_id: str,
        program: dict[str, Any],
        *,
        plan: list[dict[str, Any]],
        active_stage: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        if not plan:
            return [], None

        created: list[dict[str, Any]] = []
        for idx, step in enumerate(plan):
            step_stage = str(step.get("stage") or "")
            if active_stage and step_stage and step_stage != active_stage:
                continue
            key = self._work_step_key(step, idx)
            if self._has_any_work_key(flow_id, key):
                continue
            if not self._when_matches(step.get("when"), flow_id=flow_id, program=program):
                continue
            request, step_err = self._enqueue_work_step(
                flow_id=flow_id,
                program=program,
                step=step,
                step_idx=idx,
            )
            if step_err is not None:
                return [], step_err
            if request is not None:
                created.append(request)
        return created, None

    @staticmethod
    def _work_request_from_row(row: _WorkRow) -> dict[str, Any]:
        work_input = json.loads(row.input_json)
        return {
            "work_id": row.work_id,
            "kind": row.kind,
            "executor_class": str(work_input.get("_executor_class") or "unspecified"),
            "suggested_executor_id": str(work_input.get("_executor_id") or ""),
            "input": {"id": work_input.get("item_id")},
            "output_contract": json.loads(row.output_contract_json),
            "quality_gates": work_input.get("_quality_gates") or {},
            "escalate_if": work_input.get("_escalate_if") or [],
        }

    def _apply_work_result(
        self, flow_id: str, work_result: dict[str, Any],
    ) -> tuple[dict[str, Any], Optional[dict[str, str]]]:
        work_id = str(work_result.get("work_id") or "")
        if not work_id:
            return {}, {"code": "missing_required_field", "message": "work_result.work_id is required"}

        work = self._get_work(flow_id, work_id)
        if work is None:
            return {}, {"code": "invalid_input", "message": f"Unknown work_id: {work_id}"}
        if work.status != "requested":
            return {}, {"code": "invalid_input", "message": f"work_id is not pending: {work_id}"}

        status = str(work_result.get("status") or "ok")
        if status not in {"ok", "failed", "partial"}:
            return {}, {"code": "invalid_input", "message": f"Invalid work_result.status: {status}"}

        now = self._now()
        if status != "ok":
            self._conn.execute(
                "UPDATE continue_work SET status = 'failed', result_json = ?, updated_at = ? WHERE work_id = ?",
                (json.dumps(work_result, ensure_ascii=False), now, work_id),
            )
            return {"work_id": work_id, "status": "failed"}, None

        work_input = json.loads(work.input_json)
        applied, err = self._apply_work_outputs(work_input, work_result)
        if err is not None:
            return {}, err
        self._conn.execute(
            "UPDATE continue_work SET status = 'completed', result_json = ?, updated_at = ? WHERE work_id = ?",
            (json.dumps(work_result, ensure_ascii=False), now, work_id),
        )
        return {"work_id": work_id, "status": "applied", "ops": applied}, None

    @staticmethod
    def _resolve_mutation_value(value: Any, *, outputs: dict[str, Any], work_input: dict[str, Any]) -> Any:
        if isinstance(value, str):
            if value.startswith("$output."):
                return outputs.get(value[8:])
            if value.startswith("$input."):
                return work_input.get(value[7:])
            return value
        if isinstance(value, list):
            return [
                LocalContinuationRuntime._resolve_mutation_value(v, outputs=outputs, work_input=work_input)
                for v in value
            ]
        if isinstance(value, dict):
            return {
                str(k): LocalContinuationRuntime._resolve_mutation_value(v, outputs=outputs, work_input=work_input)
                for k, v in value.items()
            }
        return value

    def _mutation_ops_from_work(
        self, work_input: dict[str, Any], outputs: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        apply_spec = work_input.get("apply") or {}
        if not isinstance(apply_spec, dict):
            return [], {"code": "invalid_input", "message": "work input apply spec must be an object"}
        ops = apply_spec.get("ops")
        if ops is None:
            return [], None
        if not isinstance(ops, list):
            return [], {"code": "invalid_input", "message": "apply.ops must be a list"}

        default_target = work_input.get("item_id")
        normalized: list[dict[str, Any]] = []
        for raw in ops:
            if not isinstance(raw, dict):
                return [], {"code": "invalid_input", "message": "apply.ops entries must be objects"}
            op = {str(k): v for k, v in raw.items()}
            op_name = str(op.get("op") or "")
            if not op_name:
                return [], {"code": "invalid_input", "message": "mutation op requires op field"}
            if "target" not in op and default_target:
                op["target"] = default_target

            if "from_output" in op:
                return [], {
                    "code": "invalid_input",
                    "message": "from_output is not supported; use explicit $output.* values",
                }

            resolved = {
                str(k): self._resolve_mutation_value(v, outputs=outputs, work_input=work_input)
                for k, v in op.items()
            }
            normalized.append(resolved)
        return normalized, None

    def _apply_mutation_ops(
        self, ops: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        applied: list[dict[str, Any]] = []
        for op in ops:
            op_name = str(op.get("op") or "")
            target = op.get("target")
            if op_name == "upsert_item":
                if not target:
                    return [], {"code": "missing_required_field", "message": "upsert_item requires target"}
                content = op.get("content")
                if content is None:
                    return [], {"code": "missing_required_field", "message": "upsert_item requires content"}
                tags = op.get("tags")
                if tags is not None and not isinstance(tags, dict):
                    return [], {"code": "invalid_input", "message": "upsert_item.tags must be an object"}
                summary = op.get("summary")
                self._keeper.put(
                    content=str(content),
                    id=str(target),
                    tags=tags,
                    summary=str(summary) if summary is not None else None,
                )
            elif op_name == "set_tags":
                if not target:
                    return [], {"code": "missing_required_field", "message": "set_tags requires target"}
                tags = op.get("tags")
                if not isinstance(tags, dict):
                    return [], {"code": "missing_required_field", "message": "set_tags requires tags object"}
                normalized_tags = {str(k): v for k, v in tags.items()}
                self._keeper.tag(str(target), tags=normalized_tags)
            elif op_name == "set_summary":
                if not target:
                    return [], {"code": "missing_required_field", "message": "set_summary requires target"}
                summary = op.get("summary")
                if summary is None:
                    return [], {"code": "missing_required_field", "message": "set_summary requires summary"}
                existing = self._keeper.get(str(target))
                if existing is None:
                    return [], {"code": "invalid_input", "message": f"Target note not found: {target}"}
                from .processors import ProcessorResult

                doc_coll = self._keeper._resolve_doc_collection()
                result = ProcessorResult(task_type="summarize", summary=str(summary))
                self._keeper.apply_result(
                    str(target),
                    doc_coll,
                    result,
                    existing_tags=dict(existing.tags),
                )
            else:
                return [], {"code": "invalid_input", "message": f"Unsupported mutation op: {op_name}"}
            applied.append({"op": op_name, "target": str(target), "status": "applied"})
        return applied, None

    def _apply_work_outputs(
        self, work_input: dict[str, Any], work_result: dict[str, Any],
    ) -> tuple[dict[str, Any], Optional[dict[str, str]]]:
        outputs = work_result.get("outputs") or {}
        if not isinstance(outputs, dict):
            return {}, {"code": "invalid_input", "message": "work_result.outputs must be an object"}
        ops, err = self._mutation_ops_from_work(work_input, outputs)
        if err is not None:
            return {}, err
        if not ops:
            return {}, None
        applied, err = self._apply_mutation_ops(ops)
        if err is not None:
            return {}, err
        return {"ops": applied}, None

    def _build_frame(
        self,
        flow_id: str,
        note_id: Optional[str],
        status: str,
        goal: str,
        stage: str,
        program: dict[str, Any],
        evidence: list[dict[str, Any]],
        budget_used: Optional[dict[str, Any]] = None,
        extra_slots: Optional[dict[str, Any]] = None,
        discriminators: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        params = program.get("params") if isinstance(program.get("params"), dict) else {}
        query_text = str(params.get("text") or "")
        success_tests = []
        raw_tests = program.get("success_tests")
        if isinstance(raw_tests, list):
            success_tests = [str(x) for x in raw_tests if str(x).strip()]

        task = "Continue flow"
        if goal:
            task = f"Goal: {goal}"
        if note_id:
            task = f"{task} (note={note_id})"
        if query_text:
            task = f"{task} (query={query_text})"
        slots = {
            "flow_id": flow_id,
            "note_id": note_id,
            "goal": goal,
            "stage": stage,
            "query": query_text,
            "success_tests": success_tests,
            "template_ref": str(program.get("template_ref") or ""),
        }
        if isinstance(extra_slots, dict):
            for key, value in extra_slots.items():
                slots[str(key)] = value
        return {
            "slots": slots,
            "views": {
                "task": task,
                "evidence": evidence,
                "hygiene": [],
                "discriminators": (
                    discriminators
                    if isinstance(discriminators, dict)
                    else self._empty_discriminators()
                ),
            },
            "budget_used": {
                "tokens": self._as_int((budget_used or {}).get("tokens"), 0),
                "nodes": self._as_int((budget_used or {}).get("nodes"), 0),
            },
            "status": status,
        }

    @staticmethod
    def _render_evidence_block(evidence: list[dict[str, Any]], *, limit: int = 10) -> str:
        if not evidence:
            return "No evidence."
        lines = []
        for row in evidence[:limit]:
            score = row.get("score")
            score_str = f" ({score:.2f})" if isinstance(score, (int, float)) else ""
            lines.append(f"- {row.get('id', '')}{score_str}  {row.get('summary', '')}")
        return "\n".join(lines)

    @staticmethod
    def _split_frontmatter(content: str) -> tuple[dict[str, Any], str]:
        if not content.startswith("---"):
            return {}, content
        parts = content.split("---", 2)
        if len(parts) < 3:
            return {}, content
        body = parts[2].lstrip("\n")
        try:
            import yaml

            frontmatter = yaml.safe_load(parts[1])
        except Exception:
            return {}, body
        if not isinstance(frontmatter, dict):
            return {}, body
        return frontmatter, body

    def _load_template_spec(self, template_ref: str) -> Optional[dict[str, Any]]:
        if not template_ref:
            return None
        doc = self._keeper.get(template_ref)
        if doc is None:
            return None
        raw = str(doc.summary or "")
        frontmatter, body = self._split_frontmatter(raw)
        template_text = body
        try:
            from .analyzers import extract_prompt_section

            prompt_section = extract_prompt_section(body)
            if prompt_section:
                template_text = prompt_section
        except Exception:
            pass

        render_override = frontmatter.get("render")
        if isinstance(render_override, str) and render_override.strip():
            template_text = render_override
        bindings = frontmatter.get("bindings")
        if not isinstance(bindings, dict):
            bindings = {}

        template_frame_request = frontmatter.get("frame_request")
        if not isinstance(template_frame_request, dict):
            template_frame_request = None
        template_policy = frontmatter.get("decision_policy")
        if not isinstance(template_policy, dict):
            template_policy = None
        template_stages = self._normalize_profile_stages(frontmatter.get("stages"))
        return {
            "template_ref": template_ref,
            "template_text": template_text,
            "bindings": dict(bindings),
            "frame_request": template_frame_request,
            "decision_policy": template_policy,
            "stages": template_stages,
        }

    @staticmethod
    def _empty_discriminators() -> dict[str, Any]:
        return {
            "version": DECISION_SUPPORT_VERSION,
            "planner_priors": {
                "fanout": {},
                "selectivity": {},
                "cardinality": {},
            },
            "query_stats": {
                "lane_entropy": 0.0,
                "top1_top2_margin": 0.0,
                "pivot_coverage_topk": 0.0,
                "expansion_yield_prev_step": 0.0,
                "cost_per_gain_prev_step": 0.0,
                "temporal_alignment": 0.0,
            },
            "policy_hint": {
                "strategy": "explore_more",
                "reason_codes": ["insufficient_signal"],
            },
            "staleness": {"stats_age_s": None, "fallback_mode": True},
        }

    @staticmethod
    def _as_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
        return max(low, min(high, value))

    def _decision_policy_from_program(self, program: dict[str, Any]) -> dict[str, Any]:
        policy: Any = program.get("decision_policy")
        if not isinstance(policy, dict):
            params = program.get("params") if isinstance(program.get("params"), dict) else {}
            policy = params.get("decision_policy") if isinstance(params, dict) else None
        if not isinstance(policy, dict):
            policy = {}

        merged = dict(DEFAULT_DECISION_POLICY)
        merged["margin_high"] = self._clamp(self._as_float(policy.get("margin_high"), merged["margin_high"]))
        merged["entropy_low"] = self._clamp(self._as_float(policy.get("entropy_low"), merged["entropy_low"]))
        merged["margin_low"] = self._clamp(self._as_float(policy.get("margin_low"), merged["margin_low"]))
        merged["entropy_high"] = self._clamp(self._as_float(policy.get("entropy_high"), merged["entropy_high"]))
        merged["lineage_strong"] = self._clamp(
            self._as_float(policy.get("lineage_strong"), merged["lineage_strong"]),
        )
        merged["pivot_topk"] = min(max(self._as_int(policy.get("pivot_topk"), merged["pivot_topk"]), 1), 20)
        merged["max_pivots"] = min(max(self._as_int(policy.get("max_pivots"), merged["max_pivots"]), 1), 5)
        return merged

    @staticmethod
    def _empty_lineage_signal() -> dict[str, Any]:
        return {
            "coverage_topk": 0.0,
            "dominant_concentration_topk": 0.0,
            "dominant": "",
            "distinct_topk": 0,
        }

    def _lineage_signal(
        self, evidence: list[dict[str, Any]], *, field: str, topk: int,
    ) -> dict[str, Any]:
        window = evidence[:topk]
        if not window:
            return self._empty_lineage_signal()

        counts: dict[str, int] = {}
        present = 0
        for row in window:
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata")
            focus = metadata.get("focus") if isinstance(metadata, dict) else None
            if not isinstance(focus, dict):
                continue
            raw = focus.get(field)
            lineage = str(raw or "").strip()
            if not lineage:
                continue
            present += 1
            counts[lineage] = counts.get(lineage, 0) + 1

        if not counts:
            return self._empty_lineage_signal()

        dominant, dominant_count = max(counts.items(), key=lambda item: item[1])
        coverage = self._clamp(present / max(1, len(window)))
        concentration = self._clamp(dominant_count / max(1, present))
        return {
            "coverage_topk": round(coverage, 4),
            "dominant_concentration_topk": round(concentration, 4),
            "dominant": dominant,
            "distinct_topk": int(len(counts)),
        }

    def _lineage_signals(self, evidence: list[dict[str, Any]], *, topk: int) -> dict[str, Any]:
        return {
            "version": self._lineage_signal(evidence, field="version", topk=topk),
            "part": self._lineage_signal(evidence, field="part", topk=topk),
        }

    def _decision_override_from_payload(
        self, payload: dict[str, Any],
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, str]]]:
        override = payload.get("decision_override")
        if override is None:
            return None, None
        if not isinstance(override, dict):
            return None, {
                "code": "invalid_input",
                "message": "decision_override must be an object",
            }
        strategy = str(override.get("strategy") or "").strip()
        if strategy not in DECISION_STRATEGIES:
            return None, {
                "code": "invalid_input",
                "message": f"Unsupported decision_override.strategy: {strategy}",
            }
        return {
            "strategy": strategy,
            "reason": str(override.get("reason") or ""),
        }, None

    def _candidate_subject_keys(
        self, frame_request: dict[str, Any], evidence: list[dict[str, Any]],
    ) -> list[str]:
        candidates: list[str] = []
        where_tags = self._parse_where_tags(frame_request)
        for key in where_tags:
            k = str(key).strip()
            if k and k not in candidates:
                candidates.append(k)
        for row in evidence[:12]:
            metadata = row.get("metadata") if isinstance(row, dict) else None
            tags = metadata.get("tags") if isinstance(metadata, dict) else None
            if not isinstance(tags, dict):
                continue
            for key in sorted(tags.keys()):
                k = str(key).strip()
                if k and k not in candidates:
                    candidates.append(k)
                if len(candidates) >= 12:
                    break
            if len(candidates) >= 12:
                break

        tag_kind_cache: dict[str, str] = {}
        facets: list[str] = []
        edges: list[str] = []
        for key in candidates:
            kind = self._tag_key_kind(key, cache=tag_kind_cache)
            if kind == "edge":
                edges.append(key)
            else:
                facets.append(key)
        return facets + edges

    def _tag_key_kind(self, key: str, *, cache: Optional[dict[str, str]] = None) -> str:
        k = str(key or "").strip()
        if not k:
            return "facet"
        if cache is not None and k in cache:
            return cache[k]

        kind = "facet"
        try:
            doc_coll = self._keeper._resolve_doc_collection()
            tagdoc = self._keeper._document_store.get(doc_coll, f".tag/{k}")
            tags = tagdoc.tags if tagdoc and isinstance(getattr(tagdoc, "tags", None), dict) else {}
            inverse = str(tags.get("_inverse") or "").strip()
            if inverse:
                kind = "edge"
        except Exception:
            kind = "facet"

        if cache is not None:
            cache[k] = kind
        return kind

    @staticmethod
    def _best_fact_from_counts(
        counts: dict[tuple[str, str], int], *, total: int,
    ) -> Optional[str]:
        if not counts:
            return None
        (key, value), count = max(
            counts.items(),
            key=lambda item: (item[1], item[0][0], item[0][1]),
        )
        if count < 2 or (count / max(total, 1)) < 0.4:
            return None
        return f"{key}={value}"

    def _tag_profile(self, evidence: list[dict[str, Any]], *, topk: int) -> dict[str, Any]:
        window = evidence[:topk]
        if not window:
            return {
                "edge_key_count": 0,
                "facet_key_count": 0,
                "edge_keys": [],
                "facet_keys": [],
            }

        keyset: set[str] = set()
        for row in window:
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata")
            tags = metadata.get("tags") if isinstance(metadata, dict) else None
            if not isinstance(tags, dict):
                continue
            for key in tags:
                k = str(key).strip()
                if not k or k.startswith("_"):
                    continue
                keyset.add(k)

        tag_kind_cache: dict[str, str] = {}
        edge_keys: list[str] = []
        facet_keys: list[str] = []
        for key in sorted(keyset):
            if self._tag_key_kind(key, cache=tag_kind_cache) == "edge":
                edge_keys.append(key)
            else:
                facet_keys.append(key)
        return {
            "edge_key_count": len(edge_keys),
            "facet_key_count": len(facet_keys),
            "edge_keys": edge_keys,
            "facet_keys": facet_keys,
        }

    def _query_stats(
        self,
        *,
        frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
        previous_evidence_ids: list[str],
        topk: int,
    ) -> dict[str, float]:
        window = evidence[:topk]
        scores: list[float] = []
        for idx, row in enumerate(window):
            raw = row.get("score")
            if isinstance(raw, (int, float)):
                scores.append(float(raw))
            else:
                scores.append(float(max(topk - idx, 1)))

        if len(scores) >= 2:
            ranked = sorted(scores, reverse=True)
            denom = max(abs(ranked[0]), 1e-9)
            top_margin = self._clamp((ranked[0] - ranked[1]) / denom)
        elif len(scores) == 1:
            top_margin = 1.0
        else:
            top_margin = 0.0

        if scores:
            weights = [max(score, 0.0) for score in scores]
            total = sum(weights)
            if total <= 0:
                weights = [1.0 / (idx + 1) for idx in range(len(scores))]
                total = sum(weights)
            probs = [w / total for w in weights if total > 0]
            if len(probs) <= 1:
                lane_entropy = 0.0
            else:
                entropy = -sum(p * math.log(p) for p in probs if p > 0)
                lane_entropy = self._clamp(entropy / math.log(len(probs)))
        else:
            lane_entropy = 0.0

        where_keys = set(self._parse_where_tags(frame_request).keys())
        if not where_keys:
            counts: dict[str, int] = {}
            for row in window:
                metadata = row.get("metadata") if isinstance(row, dict) else None
                tags = metadata.get("tags") if isinstance(metadata, dict) else None
                if not isinstance(tags, dict):
                    continue
                for key, value in tags.items():
                    if value in (None, "", [], {}):
                        continue
                    k = str(key)
                    counts[k] = counts.get(k, 0) + 1
            if counts:
                where_keys = {max(counts, key=counts.get)}

        covered = 0
        for row in window:
            metadata = row.get("metadata") if isinstance(row, dict) else None
            tags = metadata.get("tags") if isinstance(metadata, dict) else None
            if not isinstance(tags, dict):
                continue
            if any(tags.get(key) not in (None, "", [], {}) for key in where_keys):
                covered += 1
        pivot_coverage = self._clamp(covered / max(1, len(window)))

        current_ids = [str(row.get("id")) for row in evidence if row.get("id")]
        prev_ids = {str(item) for item in previous_evidence_ids if item}
        gain = len([eid for eid in current_ids if eid not in prev_ids])
        expansion_yield = self._clamp(gain / max(1, len(current_ids)))
        cost_per_gain = float(len(current_ids)) if gain == 0 else float(len(current_ids)) / gain

        now = datetime.now(timezone.utc)
        recent_count = 0
        dated_count = 0
        for row in window:
            metadata = row.get("metadata") if isinstance(row, dict) else None
            updated = metadata.get("updated") if isinstance(metadata, dict) else None
            if not isinstance(updated, str) or not updated.strip():
                continue
            try:
                dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except Exception:
                continue
            dated_count += 1
            age_days = (now - dt.astimezone(timezone.utc)).total_seconds() / 86400.0
            if age_days <= 365.0:
                recent_count += 1
        temporal_alignment = 0.5 if dated_count == 0 else self._clamp(recent_count / dated_count)

        return {
            "lane_entropy": round(lane_entropy, 4),
            "top1_top2_margin": round(top_margin, 4),
            "pivot_coverage_topk": round(pivot_coverage, 4),
            "expansion_yield_prev_step": round(expansion_yield, 4),
            "cost_per_gain_prev_step": round(cost_per_gain, 4),
            "temporal_alignment": round(temporal_alignment, 4),
        }

    def _choose_strategy(
        self,
        *,
        query_stats: dict[str, float],
        lineage: dict[str, Any],
        policy: dict[str, Any],
        staleness: dict[str, Any],
        decision_override: Optional[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        if decision_override is not None:
            reason = str(decision_override.get("reason") or "").strip()
            codes = ["override"] if not reason else [f"override:{reason}"]
            return str(decision_override["strategy"]), codes

        margin = self._as_float(query_stats.get("top1_top2_margin"), 0.0)
        entropy = self._as_float(query_stats.get("lane_entropy"), 1.0)
        margin_high = self._as_float(policy.get("margin_high"), DEFAULT_DECISION_POLICY["margin_high"])
        entropy_low = self._as_float(policy.get("entropy_low"), DEFAULT_DECISION_POLICY["entropy_low"])
        margin_low = self._as_float(policy.get("margin_low"), DEFAULT_DECISION_POLICY["margin_low"])
        entropy_high = self._as_float(policy.get("entropy_high"), DEFAULT_DECISION_POLICY["entropy_high"])
        lineage_strong = self._as_float(policy.get("lineage_strong"), DEFAULT_DECISION_POLICY["lineage_strong"])

        version = lineage.get("version") if isinstance(lineage, dict) else {}
        part = lineage.get("part") if isinstance(lineage, dict) else {}
        version_dom = self._as_float(
            version.get("dominant_concentration_topk") if isinstance(version, dict) else None, 0.0,
        )
        part_dom = self._as_float(
            part.get("dominant_concentration_topk") if isinstance(part, dict) else None, 0.0,
        )
        lineage_dom = max(version_dom, part_dom)
        lineage_kind = "version" if version_dom >= part_dom else "part"

        reasons: list[str] = []
        strategy = "explore_more"
        if margin >= margin_high and entropy <= entropy_low:
            strategy = "single_lane_refine"
            reasons = ["high_margin", "low_entropy"]
        elif lineage_dom >= lineage_strong and entropy < entropy_high:
            strategy = "single_lane_refine"
            reasons = [f"strong_{lineage_kind}_lineage"]
        elif margin <= margin_low or entropy >= entropy_high:
            strategy = "top2_plus_bridge"
            reasons = ["low_margin" if margin <= margin_low else "high_entropy"]
        else:
            reasons = ["mixed_signal"]

        if bool(staleness.get("fallback_mode")):
            reasons.append("planner_fallback")
        return strategy, reasons

    @staticmethod
    def _pivot_ids(evidence: list[dict[str, Any]], *, strategy: str, max_pivots: int) -> list[str]:
        ids = [str(row.get("id")) for row in evidence if row.get("id")]
        if strategy == "single_lane_refine":
            return ids[: min(1, max_pivots)]
        if strategy == "top2_plus_bridge":
            return ids[: min(2, max_pivots)]
        return []

    def _decision_capsule(
        self,
        *,
        program: dict[str, Any],
        frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
        previous_evidence_ids: list[str],
        decision_override: Optional[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        planner_payload = {
            "planner_priors": {
                "fanout": {},
                "selectivity": {},
                "cardinality": {},
            },
            "staleness": {"stats_age_s": None, "fallback_mode": True},
        }
        try:
            params = program.get("params") if isinstance(program.get("params"), dict) else {}
            scope_key = params.get("scope_key") if isinstance(params, dict) else None
            candidates = self._candidate_subject_keys(frame_request, evidence)
            planner_payload = self._keeper.get_planner_priors(
                scope_key=scope_key if isinstance(scope_key, str) and scope_key else None,
                candidates=candidates or None,
            )
        except Exception:
            logger.debug("Planner priors unavailable for decision capsule", exc_info=True)

        priors = planner_payload.get("planner_priors") if isinstance(planner_payload, dict) else {}
        if not isinstance(priors, dict):
            priors = {}
        staleness = planner_payload.get("staleness") if isinstance(planner_payload, dict) else {}
        if not isinstance(staleness, dict):
            staleness = {"stats_age_s": None, "fallback_mode": True}

        policy = self._decision_policy_from_program(program)
        topk = self._as_int(policy.get("pivot_topk"), DEFAULT_DECISION_POLICY["pivot_topk"])
        query_stats = self._query_stats(
            frame_request=frame_request,
            evidence=evidence,
            previous_evidence_ids=previous_evidence_ids,
            topk=topk,
        )
        lineage = self._lineage_signals(evidence, topk=topk)
        tag_profile = self._tag_profile(evidence, topk=topk)
        strategy, reason_codes = self._choose_strategy(
            query_stats=query_stats,
            lineage=lineage,
            policy=policy,
            staleness=staleness,
            decision_override=decision_override,
        )
        pivot_ids = self._pivot_ids(
            evidence,
            strategy=strategy,
            max_pivots=self._as_int(policy.get("max_pivots"), DEFAULT_DECISION_POLICY["max_pivots"]),
        )

        discriminators = {
            "version": DECISION_SUPPORT_VERSION,
            "planner_priors": {
                "fanout": priors.get("fanout", {}) if isinstance(priors.get("fanout"), dict) else {},
                "selectivity": priors.get("selectivity", {}) if isinstance(priors.get("selectivity"), dict) else {},
                "cardinality": priors.get("cardinality", {}) if isinstance(priors.get("cardinality"), dict) else {},
            },
            "query_stats": query_stats,
            "lineage": lineage,
            "tag_profile": tag_profile,
            "policy_hint": {
                "strategy": strategy,
                "reason_codes": reason_codes,
            },
            "staleness": {
                "stats_age_s": staleness.get("stats_age_s"),
                "fallback_mode": bool(staleness.get("fallback_mode")),
            },
        }
        snapshot = {
            "version": DECISION_SUPPORT_VERSION,
            "strategy_chosen": strategy,
            "reason_codes": reason_codes,
            "pivot_ids": pivot_ids,
        }
        return discriminators, snapshot

    def _dominant_tag_fact(self, evidence: list[dict[str, Any]]) -> Optional[str]:
        if not evidence:
            return None
        rows = evidence[:10]
        total = max(len(rows), 1)
        facet_counts: dict[tuple[str, str], int] = {}
        edge_counts: dict[tuple[str, str], int] = {}
        tag_kind_cache: dict[str, str] = {}

        for row in rows:
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata")
            tags = metadata.get("tags") if isinstance(metadata, dict) else None
            if not isinstance(tags, dict):
                continue
            for key, raw in tags.items():
                k = str(key).strip()
                if not k or k.startswith("_") or k in NON_PIVOT_TAG_KEYS:
                    continue
                kind = self._tag_key_kind(k, cache=tag_kind_cache)
                values: list[str] = []
                if isinstance(raw, list):
                    values = [str(v) for v in raw if isinstance(v, (str, int, float, bool))]
                elif isinstance(raw, (str, int, float, bool)):
                    values = [str(raw)]
                for val in values[:4]:
                    if not val.strip():
                        continue
                    if kind == "edge":
                        edge_counts[(k, val)] = edge_counts.get((k, val), 0) + 1
                    else:
                        facet_counts[(k, val)] = facet_counts.get((k, val), 0) + 1

        # Facets provide grouping/scoping; edges are fallback pivots if no strong facet.
        fact = self._best_fact_from_counts(facet_counts, total=total)
        if fact:
            return fact
        return self._best_fact_from_counts(edge_counts, total=total)

    def _query_auto_next_frame_request(
        self,
        *,
        current_frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
        decision_snapshot: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        seed = current_frame_request.get("seed") if isinstance(current_frame_request, dict) else {}
        if not isinstance(seed, dict):
            return None
        if str(seed.get("mode") or "") != "query" or not str(seed.get("value") or "").strip():
            return None

        budget = current_frame_request.get("budget")
        if not isinstance(budget, dict):
            budget = {}
        options = current_frame_request.get("options")
        if not isinstance(options, dict):
            options = {}
        metadata_level = self._normalize_metadata_level(options.get("metadata"))
        limit = self._pipeline_limit(current_frame_request, default_limit=10)
        strategy = str(decision_snapshot.get("strategy_chosen") or "explore_more")

        if strategy == "single_lane_refine":
            fact = self._dominant_tag_fact(evidence)
            pipeline: list[dict[str, Any]] = []
            if fact:
                pipeline.append({"op": "where", "args": {"facts": [fact]}})
            pipeline.append({"op": "slice", "args": {"limit": limit}})
            return {
                "seed": {"mode": "query", "value": str(seed.get("value"))},
                "pipeline": pipeline,
                "budget": dict(budget),
                "options": {"deep": True, "metadata": metadata_level},
            }

        if strategy == "top2_plus_bridge":
            return {
                "seed": {"mode": "query", "value": str(seed.get("value"))},
                "pipeline": [{"op": "slice", "args": {"limit": limit}}],
                "budget": dict(budget),
                "options": {"deep": True, "metadata": metadata_level},
            }

        # explore_more: broaden modestly then re-evaluate.
        broaden_limit = min(max(limit + 5, 1), 50)
        return {
            "seed": {"mode": "query", "value": str(seed.get("value"))},
            "pipeline": [{"op": "slice", "args": {"limit": broaden_limit}}],
            "budget": dict(budget),
            "options": {"deep": True, "metadata": metadata_level},
        }

    def _top_tag_facts(
        self, evidence: list[dict[str, Any]], *, max_facts: int = 2,
    ) -> list[str]:
        if not evidence or max_facts <= 0:
            return []
        rows = evidence[:10]
        total = max(len(rows), 1)
        facet_counts: dict[tuple[str, str], int] = {}
        edge_counts: dict[tuple[str, str], int] = {}
        tag_kind_cache: dict[str, str] = {}

        for row in rows:
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata")
            tags = metadata.get("tags") if isinstance(metadata, dict) else None
            if not isinstance(tags, dict):
                continue
            for key, raw in tags.items():
                k = str(key).strip()
                if not k or k.startswith("_") or k in NON_PIVOT_TAG_KEYS:
                    continue
                kind = self._tag_key_kind(k, cache=tag_kind_cache)
                values: list[str] = []
                if isinstance(raw, list):
                    values = [str(v) for v in raw if isinstance(v, (str, int, float, bool))]
                elif isinstance(raw, (str, int, float, bool)):
                    values = [str(raw)]
                for val in values[:4]:
                    if not val.strip():
                        continue
                    if kind == "edge":
                        edge_counts[(k, val)] = edge_counts.get((k, val), 0) + 1
                    else:
                        facet_counts[(k, val)] = facet_counts.get((k, val), 0) + 1

        def _ordered(counts: dict[tuple[str, str], int]) -> list[str]:
            ranked = sorted(
                counts.items(),
                key=lambda item: (item[1], item[0][0], item[0][1]),
                reverse=True,
            )
            out: list[str] = []
            for (key, value), count in ranked:
                if count < 2 or (count / total) < 0.4:
                    continue
                out.append(f"{key}={value}")
                if len(out) >= max_facts:
                    break
            return out

        facts: list[str] = []
        for fact in _ordered(facet_counts):
            if fact not in facts:
                facts.append(fact)
            if len(facts) >= max_facts:
                return facts
        for fact in _ordered(edge_counts):
            if fact not in facts:
                facts.append(fact)
            if len(facts) >= max_facts:
                return facts
        return facts

    @staticmethod
    def _query_auto_branch_utility(
        *, discriminators: dict[str, Any], evidence_count: int,
    ) -> float:
        query_stats = discriminators.get("query_stats") if isinstance(discriminators, dict) else {}
        lineage = discriminators.get("lineage") if isinstance(discriminators, dict) else {}
        margin = 0.0
        if isinstance(query_stats, dict):
            try:
                margin = float(query_stats.get("top1_top2_margin") or 0.0)
            except Exception:
                margin = 0.0
        version_dom = 0.0
        part_dom = 0.0
        if isinstance(lineage, dict):
            version = lineage.get("version")
            part = lineage.get("part")
            if isinstance(version, dict):
                try:
                    version_dom = float(version.get("dominant_concentration_topk") or 0.0)
                except Exception:
                    version_dom = 0.0
            if isinstance(part, dict):
                try:
                    part_dom = float(part.get("dominant_concentration_topk") or 0.0)
                except Exception:
                    part_dom = 0.0
        evidence_term = 0.0 if evidence_count <= 0 else min(float(evidence_count) / 10.0, 1.0)
        return round(margin + (0.3 * max(version_dom, part_dom)) + (0.05 * evidence_term), 6)

    def _query_auto_top2_plus_bridge_branches(
        self,
        *,
        current_frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        seed = current_frame_request.get("seed") if isinstance(current_frame_request, dict) else {}
        if not isinstance(seed, dict):
            return []
        if str(seed.get("mode") or "") != "query" or not str(seed.get("value") or "").strip():
            return []

        budget = current_frame_request.get("budget")
        if not isinstance(budget, dict):
            budget = {}
        options = current_frame_request.get("options")
        if not isinstance(options, dict):
            options = {}
        metadata_level = self._normalize_metadata_level(options.get("metadata"))
        limit = self._pipeline_limit(current_frame_request, default_limit=10)
        query_value = str(seed.get("value"))

        branches: list[dict[str, Any]] = []
        top_facts = self._top_tag_facts(evidence, max_facts=2)
        for idx, fact in enumerate(top_facts, start=1):
            branches.append(
                {
                    "id": f"pivot_{idx}",
                    "kind": "pivot",
                    "frame_request": {
                        "seed": {"mode": "query", "value": query_value},
                        "pipeline": [
                            {"op": "where", "args": {"facts": [fact]}},
                            {"op": "slice", "args": {"limit": limit}},
                        ],
                        "budget": dict(budget),
                        "options": {"deep": True, "metadata": metadata_level},
                    },
                }
            )

        branches.append(
            {
                "id": "bridge",
                "kind": "bridge",
                "frame_request": {
                    "seed": {"mode": "query", "value": query_value},
                    "pipeline": [{"op": "slice", "args": {"limit": limit}}],
                    "budget": dict(budget),
                    "options": {"deep": True, "metadata": metadata_level},
                },
            }
        )
        return branches[:3]

    def _resolve_template_expr(
        self,
        value: Any,
        *,
        program: dict[str, Any],
        slots: dict[str, Any],
    ) -> Any:
        if isinstance(value, str):
            def _replace(match: re.Match[str]) -> str:
                scope = match.group(1)
                key = match.group(2)
                fallback = match.group(3) or ""
                if scope == "params":
                    resolved = self._param_value(program, key)
                else:
                    resolved = slots.get(key)
                if resolved is None:
                    resolved = fallback
                if isinstance(resolved, (dict, list)):
                    return json.dumps(resolved, ensure_ascii=False)
                return str(resolved)

            return _BIND_EXPR_RE.sub(_replace, value)
        if isinstance(value, list):
            return [
                self._resolve_template_expr(v, program=program, slots=slots)
                for v in value
            ]
        if isinstance(value, dict):
            return {
                str(k): self._resolve_template_expr(v, program=program, slots=slots)
                for k, v in value.items()
            }
        return value

    def _resolve_template_binding_value(
        self,
        binding_name: str,
        binding_spec: Any,
        *,
        program: dict[str, Any],
        slots: dict[str, Any],
    ) -> tuple[Any, Optional[dict[str, str]]]:
        spec = binding_spec
        if isinstance(spec, str):
            spec = {"from": spec}
        if not isinstance(spec, dict):
            return "", {
                "code": "invalid_input",
                "message": f"Template binding {binding_name} must be an object or string",
            }

        source = spec.get("from")
        if source is not None:
            if not isinstance(source, str):
                return "", {
                    "code": "invalid_input",
                    "message": f"Template binding {binding_name}.from must be a string",
                }
            if source.startswith("params."):
                value = self._param_value(program, source[7:])
            elif source.startswith("slots."):
                value = slots.get(source[6:])
            else:
                value = source
            resolved = self._resolve_template_expr(value, program=program, slots=slots)
            return "" if resolved is None else resolved, None

        frame_request = spec.get("frame_request")
        if frame_request is not None:
            if not isinstance(frame_request, dict):
                return "", {
                    "code": "invalid_input",
                    "message": f"Template binding {binding_name}.frame_request must be an object",
                }
            resolved_request = self._resolve_template_expr(frame_request, program=program, slots=slots)
            if not isinstance(resolved_request, dict):
                return "", {
                    "code": "invalid_input",
                    "message": f"Template binding {binding_name}.frame_request resolved to invalid value",
                }
            frame_err = self._validate_frame_request(resolved_request)
            if frame_err is not None:
                return "", frame_err
            evidence = self._frame_evidence_for_request(resolved_request)
            format_mode = str(spec.get("format") or "evidence_block")
            if format_mode == "evidence":
                return evidence, None
            if format_mode == "evidence_ids":
                return [row.get("id") for row in evidence], None
            return self._render_evidence_block(evidence), None

        return "", None

    def _resolve_template_slots(
        self,
        *,
        program: dict[str, Any],
        template_spec: dict[str, Any],
    ) -> tuple[dict[str, Any], list[dict[str, str]]]:
        bindings = template_spec.get("bindings") or {}
        if not isinstance(bindings, dict):
            return {}, [{
                "code": "invalid_input",
                "message": "template bindings must be an object",
            }]
        slots: dict[str, Any] = {}
        errors: list[dict[str, str]] = []
        for name, spec in bindings.items():
            value, err = self._resolve_template_binding_value(
                str(name),
                spec,
                program=program,
                slots=slots,
            )
            if err is not None:
                errors.append(err)
                continue
            slots[str(name)] = value
        return slots, errors

    def _render_template(
        self,
        program: dict[str, Any],
        frame: dict[str, Any],
        *,
        template_spec: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        template_ref = str(program.get("template_ref") or "")
        if not template_ref:
            return None

        if template_spec is None:
            template_spec = self._load_template_spec(template_ref)
        if template_spec is None:
            return None
        template_text = str(template_spec.get("template_text") or "")

        evidence = frame.get("views", {}).get("evidence") or []
        evidence_text = self._render_evidence_block(evidence)
        rendered = template_text

        slots = frame.get("slots") or {}
        slot_values = {
            "task": str(frame.get("views", {}).get("task") or ""),
            "evidence": evidence_text,
            **{str(k): slots[k] for k in slots},
        }
        for key in slot_values:
            value = slot_values[key]
            if isinstance(value, (dict, list)):
                sval = json.dumps(value, ensure_ascii=False)
            else:
                sval = str(value)
            rendered = rendered.replace(f"{{{{{key}}}}}", sval)

        template_hash = hashlib.sha256(template_text.encode("utf-8")).hexdigest()
        policy_seed = str(getattr(self._keeper._config, "system_docs_hash", "") or "")
        metaschema_hash = hashlib.sha256(policy_seed.encode("utf-8")).hexdigest()
        return {
            "template_ref": template_ref,
            "template_hash": f"sha256:{template_hash}",
            "metaschema_hash": f"sha256:{metaschema_hash}",
            "text": rendered,
        }

    def _apply_inline_write(
        self, state: dict[str, Any], program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        if state.get("frontier", {}).get("write_applied"):
            return [], None

        if self._goal_from_program(program) != "write":
            return [], None
        params = program.get("params") or {}
        if not isinstance(params, dict):
            return [], {"code": "invalid_input", "message": "write goal requires params object"}

        item_id = params.get("id")
        content = params.get("content")
        if not item_id or content is None:
            return [], {
                "code": "missing_required_field",
                "message": "write goal requires params.id and params.content",
            }
        ops = [{
            "op": "upsert_item",
            "target": str(item_id),
            "content": str(content),
            "tags": params.get("tags"),
            "summary": params.get("summary"),
        }]
        applied, err = self._apply_mutation_ops(ops)
        if err is not None:
            return [], err
        state.setdefault("frontier", {})["write_applied"] = True
        return applied, None

    @staticmethod
    def _stage_for_tick(
        *,
        requested: bool,
        applied_ops: list[dict[str, Any]],
        errors: bool,
        has_evidence: bool,
    ) -> str:
        if errors:
            return "failed"
        if requested:
            return "waiting_work"
        if applied_ops:
            return "reconcile"
        if has_evidence:
            return "explore"
        return "tick"

    def continue_flow(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("continue input must be a JSON object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION!r}")

        request_id = str(payload.get("request_id") or uuid.uuid4())
        idempotency_key = payload.get("idempotency_key")
        request_hash = self._hash_json(payload)

        with self._lock:
            if idempotency_key:
                existing = self._load_idempotent(str(idempotency_key))
                if existing is not None:
                    saved_hash, saved_json = existing
                    if saved_hash != request_hash:
                        raise ValueError("idempotency key reused with different payload")
                    output = json.loads(saved_json)
                    output["request_id"] = request_id
                    return output

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                flow_id = payload.get("flow_id")
                is_new_flow = flow_id is None
                if flow_id is None:
                    flow = self._create_flow()
                else:
                    flow = self._get_flow(str(flow_id))
                    if flow is None:
                        raise ValueError(f"unknown flow_id: {flow_id}")

                assert flow is not None
                expected_version = payload.get("state_version")
                if expected_version is not None and int(expected_version) != flow.state_version:
                    self._conn.rollback()
                    state = json.loads(flow.state_json)
                    program = state.get("program") or {}
                    if not isinstance(program, dict):
                        program = {}
                    goal = self._goal_from_program(program)
                    note_id = self._infer_note_id(program) if program else None
                    evidence = self._frame_evidence(program) if program else []
                    return {
                        "schema_version": SCHEMA_VERSION,
                        "request_id": request_id,
                        "idempotency_key": idempotency_key,
                        "flow_id": flow.flow_id,
                        "state_version": flow.state_version,
                        "status": "failed",
                        "frame": self._build_frame(
                            flow.flow_id,
                            note_id,
                            "failed",
                            goal,
                            "failed",
                            program,
                            evidence,
                            state.get("budget_used") if isinstance(state, dict) else None,
                        ),
                        "requests": {"work": []},
                        "applied": {"work_ops": []},
                        "state": state,
                        "next": {
                            "recommended": "continue",
                            "reason": "Reload current state_version and retry",
                        },
                        "errors": [{
                            "code": "state_conflict",
                            "message": "state_version does not match current flow version",
                        }],
                    }

                state = json.loads(flow.state_json)
                errors: list[dict[str, str]] = []
                applied_ops: list[dict[str, Any]] = []
                template_spec: Optional[dict[str, Any]] = None
                template_slots: dict[str, Any] = {}
                resolved_plan: list[dict[str, Any]] = []
                resolved_stages: list[dict[str, Any]] = []
                process_stage: Optional[str] = None
                decision_discriminators = self._empty_discriminators()
                decision_snapshot: Optional[dict[str, Any]] = None
                used_auto_query_pending = False
                used_auto_query_branch_id: Optional[str] = None
                legacy_fields = self._legacy_input_fields(payload)
                if legacy_fields:
                    errors.append({
                        "code": "invalid_input",
                        "message": (
                            "Unsupported legacy continuation fields: "
                            + ", ".join(sorted(legacy_fields))
                            + ". Use top-level goal/profile/steps."
                        ),
                    })
                program = self._merge_program(state, payload)
                decision_override, decision_override_err = self._decision_override_from_payload(payload)
                if decision_override_err is not None:
                    errors.append(decision_override_err)
                frontier_state = state.get("frontier")
                if not isinstance(frontier_state, dict):
                    frontier_state = {}
                    state["frontier"] = frontier_state
                if (
                    program
                    and self._is_query_auto_profile(program)
                    and "frame_request" not in payload
                ):
                    branch_plan = frontier_state.get("auto_query_branch_plan")
                    pending_branches = branch_plan.get("pending") if isinstance(branch_plan, dict) else None
                    if isinstance(pending_branches, list) and pending_branches:
                        first_branch = pending_branches[0]
                        frame_req = first_branch.get("frame_request") if isinstance(first_branch, dict) else None
                        if isinstance(frame_req, dict):
                            program["frame_request"] = frame_req
                            used_auto_query_pending = True
                            branch_id = first_branch.get("id")
                            if branch_id is not None:
                                used_auto_query_branch_id = str(branch_id)
                    auto_next_frame = frontier_state.get("auto_query_next_frame_request")
                    if not used_auto_query_pending and isinstance(auto_next_frame, dict):
                        program["frame_request"] = auto_next_frame
                        used_auto_query_pending = True
                if "frame_request" in payload and isinstance(frontier_state, dict):
                    frontier_state.pop("auto_query_next_frame_request", None)
                    frontier_state.pop("auto_query_branch_plan", None)
                    frontier_state.pop("auto_query_selected_branch", None)
                    frontier_state.pop("auto_query_refined", None)
                if is_new_flow and not self._program_has_inputs(payload):
                    errors.append({
                        "code": "missing_required_field",
                        "message": "New flow requires template_ref or top-level flow fields",
                    })
                if not program:
                    errors.append({
                        "code": "missing_required_field",
                        "message": "No flow program available",
                    })
                else:
                    program["goal"] = self._goal_from_program(program)
                    template_ref = str(program.get("template_ref") or "")
                    if template_ref:
                        template_spec = self._load_template_spec(template_ref)
                    if template_ref and template_spec is None:
                        errors.append({
                            "code": "unknown_template",
                            "message": f"Template not found: {template_ref}",
                        })
                    if template_spec is not None:
                        template_frame_request = template_spec.get("frame_request")
                        if (
                            isinstance(template_frame_request, dict)
                            and not isinstance(program.get("frame_request"), dict)
                        ):
                            program["_template_frame_request"] = template_frame_request
                        template_policy = template_spec.get("decision_policy")
                        if (
                            isinstance(template_policy, dict)
                            and not isinstance(program.get("decision_policy"), dict)
                        ):
                            program["decision_policy"] = template_policy
                        template_stages = template_spec.get("stages")
                        if (
                            isinstance(template_stages, list)
                            and template_stages
                            and not isinstance(program.get("stages"), list)
                        ):
                            program["stages"] = template_stages
                    resolved_plan, resolved_stages, plan_err = self._resolved_plan_and_stages(program)
                    if plan_err is not None:
                        errors.append(plan_err)
                    frame_request = self._effective_frame_request(program)
                    program["frame_request"] = frame_request
                    frame_err = self._validate_frame_request(frame_request)
                    if frame_err is not None:
                        errors.append(frame_err)

                feedback = payload.get("feedback") or {}
                work_results = feedback.get("work_results") or []
                if not isinstance(work_results, list):
                    errors.append({
                        "code": "invalid_input",
                        "message": "feedback.work_results must be a list",
                    })
                    work_results = []

                for wr in work_results:
                    if not isinstance(wr, dict):
                        errors.append({
                            "code": "invalid_input",
                            "message": "work_result entries must be objects",
                        })
                        continue
                    applied, err = self._apply_work_result(flow.flow_id, wr)
                    if err:
                        errors.append(err)
                    elif applied:
                        applied_ops.append(applied)

                requested = self._list_requested_work(flow.flow_id)
                request_work: list[dict[str, Any]] = []
                goal = self._goal_from_program(program) if program else ""
                note_id = self._infer_note_id(program) if program else None
                if program and resolved_stages and not errors:
                    process_stage = self._active_process_stage(
                        flow_id=flow.flow_id,
                        program=program,
                        state=state,
                        stages=resolved_stages,
                    )
                previous_evidence_ids = state.get("frontier", {}).get("evidence_ids") or []
                if not isinstance(previous_evidence_ids, list):
                    previous_evidence_ids = []
                evidence = self._frame_evidence(program) if program and not errors else []
                if program and template_spec is not None and not errors:
                    slots, slot_errors = self._resolve_template_slots(
                        program=program,
                        template_spec=template_spec,
                    )
                    if slot_errors:
                        errors.extend(slot_errors)
                    else:
                        template_slots = slots
                if program and not errors:
                    write_ops, write_err = self._apply_inline_write(state, program)
                    if write_err is not None:
                        errors.append(write_err)
                    elif write_ops:
                        applied_ops.extend(write_ops)
                if program and not errors:
                    frame_request = self._effective_frame_request(program)
                    decision_discriminators, decision_snapshot = self._decision_capsule(
                        program=program,
                        frame_request=frame_request,
                        evidence=evidence,
                        previous_evidence_ids=previous_evidence_ids,
                        decision_override=decision_override,
                    )

                if not requested and not errors and not applied_ops:
                    created, err = self._plan_work(
                        flow.flow_id,
                        program,
                        plan=resolved_plan,
                        active_stage=process_stage,
                    )
                    if err:
                        errors.append(err)
                    elif created:
                        requested = self._list_requested_work(flow.flow_id)

                for row in requested:
                    request_work.append(self._work_request_from_row(row))

                status = "done"
                if errors:
                    status = "failed"
                elif requested:
                    status = "waiting_work"
                if (
                    status == "done"
                    and not errors
                    and not requested
                    and process_stage
                    and resolved_stages
                ):
                    stage_keys = self._stage_step_keys(
                        flow_id=flow.flow_id,
                        program=program,
                        stage_name=process_stage,
                        plan=resolved_plan,
                    )
                    stage_complete = all(
                        self._has_finished_work_key(flow.flow_id, key)
                        for key in stage_keys
                    )
                    if stage_complete:
                        next_stage = self._next_process_stage(
                            flow_id=flow.flow_id,
                            program=program,
                            current_stage=process_stage,
                            stages=resolved_stages,
                        )
                        if next_stage is not None:
                            process_stage = next_stage

                termination = state.setdefault("termination", {})
                idle_ticks = self._as_int(termination.get("idle_ticks"), 0)
                max_idle = self._as_int(termination.get("max_idle_ticks"), 3)
                if status == "waiting_work":
                    idle_ticks += 1
                else:
                    idle_ticks = 0
                termination["idle_ticks"] = idle_ticks
                termination["max_idle_ticks"] = max_idle
                if status == "waiting_work" and idle_ticks >= max_idle:
                    status = "paused"

                stage = self._stage_for_tick(
                    requested=bool(requested),
                    applied_ops=applied_ops,
                    errors=bool(errors),
                    has_evidence=bool(evidence),
                )
                cursor_stage = process_stage or stage

                next_step = int((state.get("cursor") or {}).get("step", 0)) + 1
                state["cursor"] = {"step": next_step, "stage": cursor_stage, "phase": stage}
                state["program"] = program
                frontier = state.setdefault("frontier", {})
                frontier["evidence_ids"] = [row.get("id") for row in evidence]
                if isinstance(decision_snapshot, dict):
                    frontier["decision_support"] = decision_snapshot
                if program and self._is_query_auto_profile(program) and not errors and not requested:
                    if used_auto_query_pending and used_auto_query_branch_id:
                        plan = frontier.get("auto_query_branch_plan")
                        if not isinstance(plan, dict):
                            plan = {"pending": [], "results": []}
                        pending = plan.get("pending")
                        if not isinstance(pending, list):
                            pending = []
                        results = plan.get("results")
                        if not isinstance(results, list):
                            results = []
                        remaining: list[dict[str, Any]] = []
                        removed = False
                        for branch in pending:
                            if not isinstance(branch, dict):
                                continue
                            bid = str(branch.get("id") or "")
                            if not removed and bid == used_auto_query_branch_id:
                                utility = self._query_auto_branch_utility(
                                    discriminators=decision_discriminators,
                                    evidence_count=len(evidence),
                                )
                                results.append(
                                    {
                                        "id": bid,
                                        "kind": str(branch.get("kind") or ""),
                                        "frame_request": branch.get("frame_request"),
                                        "utility": utility,
                                        "evidence_count": len(evidence),
                                        "reason_codes": (
                                            decision_discriminators.get("policy_hint", {}).get("reason_codes", [])
                                            if isinstance(decision_discriminators, dict)
                                            else []
                                        ),
                                    }
                                )
                                removed = True
                                continue
                            remaining.append(branch)
                        plan["pending"] = remaining
                        plan["results"] = results
                        if remaining:
                            frontier["auto_query_branch_plan"] = plan
                        else:
                            frontier.pop("auto_query_branch_plan", None)
                            best: Optional[dict[str, Any]] = None
                            for result in results:
                                if not isinstance(result, dict):
                                    continue
                                if best is None or float(result.get("utility") or 0.0) > float(best.get("utility") or 0.0):
                                    best = result
                            if isinstance(best, dict):
                                frontier["auto_query_selected_branch"] = {
                                    "id": best.get("id"),
                                    "kind": best.get("kind"),
                                    "utility": best.get("utility"),
                                }
                                best_frame = best.get("frame_request")
                                if isinstance(best_frame, dict):
                                    frontier["auto_query_next_frame_request"] = best_frame
                    elif used_auto_query_pending:
                        frontier["auto_query_refined"] = True
                        frontier.pop("auto_query_next_frame_request", None)
                    elif not bool(frontier.get("auto_query_refined")) and isinstance(decision_snapshot, dict):
                        strategy = str(decision_snapshot.get("strategy_chosen") or "")
                        if strategy == "top2_plus_bridge":
                            branches = self._query_auto_top2_plus_bridge_branches(
                                current_frame_request=self._effective_frame_request(program),
                                evidence=evidence,
                            )
                            if branches:
                                frontier["auto_query_branch_plan"] = {
                                    "pending": branches,
                                    "results": [],
                                }
                        else:
                            next_auto_frame = self._query_auto_next_frame_request(
                                current_frame_request=self._effective_frame_request(program),
                                evidence=evidence,
                                decision_snapshot=decision_snapshot,
                            )
                            if isinstance(next_auto_frame, dict):
                                frontier["auto_query_next_frame_request"] = next_auto_frame
                state.setdefault("pending", {})["work_ids"] = [row.work_id for row in requested]
                budget_used = state.get("budget_used")
                if not isinstance(budget_used, dict):
                    budget_used = {"tokens": 0, "nodes": 0}
                    state["budget_used"] = budget_used
                budget_used["tokens"] = self._as_int(budget_used.get("tokens"), 0)
                budget_used["nodes"] = self._as_int(budget_used.get("nodes"), 0) + len(evidence)

                next_hint = {
                    "recommended": "continue",
                    "reason": "Continue ticking this flow",
                }
                if status == "waiting_work":
                    next_hint = {
                        "recommended": "continue",
                        "reason": "Return completed work_results to continue",
                    }
                elif status == "done":
                    next_hint = {
                        "recommended": "stop",
                        "reason": "Flow reached a terminal state",
                    }
                elif status == "failed":
                    next_hint = {
                        "recommended": "continue",
                        "reason": "Fix input errors and retry",
                    }
                if (
                    status == "done"
                    and program
                    and self._is_query_auto_profile(program)
                    and isinstance(state.get("frontier"), dict)
                    and (
                        isinstance(state["frontier"].get("auto_query_next_frame_request"), dict)
                        or (
                            isinstance(state["frontier"].get("auto_query_branch_plan"), dict)
                            and isinstance(state["frontier"]["auto_query_branch_plan"].get("pending"), list)
                            and bool(state["frontier"]["auto_query_branch_plan"].get("pending"))
                        )
                    )
                ):
                    next_hint = {
                        "recommended": "continue",
                        "reason": "Auto query refinement branches are pending",
                    }

                frame = self._build_frame(
                    flow.flow_id,
                    note_id,
                    status,
                    goal,
                    cursor_stage,
                    program,
                    evidence,
                    budget_used,
                    template_slots,
                    decision_discriminators,
                )
                rendered = None
                if not errors:
                    rendered = self._render_template(
                        program, frame, template_spec=template_spec,
                    ) if program else None

                new_state_version = flow.state_version + 1
                state_json = json.dumps(state, ensure_ascii=False)
                now = self._now()
                self._conn.execute(
                    """
                    UPDATE continue_flows
                    SET state_version = ?, status = ?, state_json = ?, updated_at = ?
                    WHERE flow_id = ?
                    """,
                    (new_state_version, status, state_json, now, flow.flow_id),
                )
                event_payload = {
                    "status": status,
                    "applied": applied_ops,
                    "errors": errors,
                    "request_id": request_id,
                    "goal": goal,
                    "template_ref": str(program.get("template_ref") or "") if program else "",
                }
                self._conn.execute(
                    """
                    INSERT INTO continue_events(event_id, flow_id, event_type, payload_json, created_at)
                    VALUES (?, ?, 'tick', ?, ?)
                    """,
                    (
                        f"e_{uuid.uuid4().hex[:12]}",
                        flow.flow_id,
                        json.dumps(event_payload, ensure_ascii=False),
                        now,
                    ),
                )

                output = {
                    "schema_version": SCHEMA_VERSION,
                    "request_id": request_id,
                    "idempotency_key": idempotency_key,
                    "flow_id": flow.flow_id,
                    "state_version": new_state_version,
                    "status": status,
                    "frame": frame,
                    "requests": {
                        "work": request_work,
                    },
                    "applied": {
                        "work_ops": applied_ops,
                    },
                    "state": state,
                    "next": next_hint,
                    "errors": errors,
                    "output_hash": self._output_hash(
                        {
                            "flow_id": flow.flow_id,
                            "state_version": new_state_version,
                            "status": status,
                            "requests": request_work,
                            "errors": errors,
                        }
                    ),
                }
                if rendered is not None:
                    output["rendered"] = rendered
                if idempotency_key:
                    self._store_idempotent(str(idempotency_key), request_hash, output)
                self._conn.commit()
                return output
            except Exception:
                self._conn.rollback()
                raise

    def run_work(self, flow_id: str, work_id: str) -> dict[str, Any]:
        with self._lock:
            work = self._get_work(flow_id, work_id)
            if work is None:
                raise ValueError(f"unknown work_id: {work_id}")
            if work.status != "requested":
                raise ValueError(f"work_id is not pending: {work_id}")

            payload = json.loads(work.input_json)
            execution = self._work_executor.execute(payload)
            content = str(payload.get("content") or "")
            runner = payload.get("runner") or {}
            executor_id = str(payload.get("_executor_id") or runner.get("executor_id") or execution.executor_id or "local.runner")
            return {
                "work_id": work_id,
                "executor_id": executor_id,
                "status": "ok",
                "outputs": execution.outputs,
                "quality": execution.quality,
                "provenance": {
                    "input_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                    "attempt": work.attempt,
                },
            }

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
