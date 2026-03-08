"""Backend-agnostic continuation engine.

Implements a minimal `continue(input) -> output` loop with durable flow state
and idempotency over pluggable storage/execution adapters.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import threading
import uuid
from typing import Any, Optional

from .continuation_env import ContinuationRuntimeEnv
from .continuation_executor import LocalWorkExecutor, WorkExecutor
from .state_doc import EvalResult, StateDoc, parse_state_doc, evaluate_state_doc
from .continuation_policy import (
    DECISION_STRATEGIES,
    DECISION_SUPPORT_VERSION,
    DEFAULT_DECISION_POLICY,
    ContinuationDecisionPolicy,
)
from .continuation_store import (
    FlowRow,
    FlowStore,
    MutationRow,
    WorkRow,
)

logger = logging.getLogger(__name__)

ALLOWED_FRAME_OPS = {"where", "slice"}
BUILTIN_QUERY_AUTO_PROFILES = {"query.auto", ".profile/query.auto"}
SYSTEM_NOTE_PREFIX = "."
MAX_CONTINUE_PAYLOAD_BYTES = 512_000
MAX_CONTINUE_STATE_BYTES = 1_000_000
MAX_CONTINUE_WORK_INPUT_BYTES = 256_000
MAX_CONTINUE_WORK_RESULT_BYTES = 256_000
MAX_CONTINUE_EVENTS_PER_FLOW = 500
CONTINUE_RESPONSE_MODES = {"standard", "debug"}
PROGRAM_OVERRIDE_KEYS = {"params", "frame_request", "decision_policy"}

class ContinuationEngine:
    """Durable local continuation runtime."""

    def __init__(
        self,
        *,
        flow_store: FlowStore,
        env: ContinuationRuntimeEnv,
        work_executor: WorkExecutor | None = None,
    ) -> None:
        self._env = env
        self._work_executor = work_executor or LocalWorkExecutor(env)
        self._flow_store = flow_store
        self._lock = threading.RLock()
        self._decision_policy = ContinuationDecisionPolicy(
            env=env,
            parse_where_tags=self._parse_where_tags,
            pipeline_limit=self._pipeline_limit,
            normalize_metadata_level=self._normalize_metadata_level,
            as_int=self._as_int,
            is_decision_tag_key=self._is_decision_tag_key,
        )

    @staticmethod
    def _hash_json(payload: dict[str, Any]) -> str:
        material = dict(payload)
        # request_id is per-call and should not affect idempotent replay keys.
        material.pop("request_id", None)
        canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _json_size_bytes(value: Any) -> int:
        material = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return len(material.encode("utf-8"))

    @classmethod
    def _validate_json_size(
        cls, value: Any, *, max_bytes: int, label: str,
    ) -> Optional[dict[str, str]]:
        try:
            size = cls._json_size_bytes(value)
        except TypeError as exc:
            return {
                "code": "invalid_input",
                "message": f"{label} must be JSON-serializable: {exc}",
            }
        if size <= max_bytes:
            return None
        return {
            "code": "payload_too_large",
            "message": f"{label} exceeds max size ({size} > {max_bytes} bytes)",
        }

    @staticmethod
    def _is_decision_tag_key(key: str) -> bool:
        # Keep decision support generic: only ignore internal/system-prefixed tags.
        return bool(key) and not key.startswith("_")

    @staticmethod
    def _is_protected_note_id(note_id: str) -> bool:
        return bool(note_id) and note_id.startswith(SYSTEM_NOTE_PREFIX)

    @staticmethod
    def _output_hash(payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _encode_cursor(flow_id: str, state_version: int) -> str:
        payload = {"f": str(flow_id), "v": int(state_version)}
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: Any) -> Optional[tuple[str, int]]:
        token = str(cursor or "").strip()
        if not token:
            return None
        try:
            padded = token + ("=" * (-len(token) % 4))
            raw = base64.urlsafe_b64decode(padded.encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
            flow_id = str(payload.get("f") or "").strip()
            version = int(payload.get("v"))
            if not flow_id or version < 0:
                return None
            return flow_id, version
        except Exception:
            return None

    @staticmethod
    def _response_mode(payload: dict[str, Any]) -> str:
        mode = str(payload.get("response_mode") or "standard").strip().lower()
        if mode not in CONTINUE_RESPONSE_MODES:
            raise ValueError(f"Unsupported response_mode: {mode}")
        return mode

    @classmethod
    def _include_debug_fields(cls, payload: dict[str, Any]) -> bool:
        return cls._response_mode(payload) == "debug"

    @staticmethod
    def _applied_entry(
        *,
        source: str,
        work_id: Optional[str],
        op: str,
        target: Optional[str],
        status: str,
        mutation_id: Optional[str] = None,
    ) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "source": source,
            "work_id": work_id,
            "op": op,
            "target": target,
            "status": status,
        }
        if mutation_id:
            entry["mutation_id"] = mutation_id
        return entry

    @staticmethod
    def _dependency_stamp(
        *,
        frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
    ) -> str:
        # Lightweight stamp for shallow revalidation between ticks.
        material: dict[str, Any] = {
            "frame_request": frame_request if isinstance(frame_request, dict) else {},
            "evidence": [],
        }
        for row in evidence:
            if not isinstance(row, dict):
                continue
            metadata = row.get("metadata")
            focus = metadata.get("focus") if isinstance(metadata, dict) else {}
            material["evidence"].append(
                {
                    "id": row.get("id"),
                    "base_id": metadata.get("base_id") if isinstance(metadata, dict) else None,
                    "updated": metadata.get("updated") if isinstance(metadata, dict) else None,
                    "focus_version": focus.get("version") if isinstance(focus, dict) else None,
                    "focus_part": focus.get("part") if isinstance(focus, dict) else None,
                }
            )
        canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
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

    def _get_flow(self, flow_id: str) -> Optional[FlowRow]:
        return self._flow_store.get_flow(flow_id)

    def _create_flow(self) -> FlowRow:
        state_json = json.dumps(self._initial_state(), ensure_ascii=False)
        return self._flow_store.create_flow(state_json)

    def _load_idempotent(self, key: str) -> Optional[tuple[str, str]]:
        return self._flow_store.load_idempotent(key)

    def _store_idempotent(self, key: str, request_hash: str, output: dict[str, Any]) -> None:
        self._flow_store.store_idempotent(
            key,
            request_hash,
            json.dumps(output, ensure_ascii=False),
        )

    def _prune_events(self, flow_id: str, *, keep_last: int = MAX_CONTINUE_EVENTS_PER_FLOW) -> None:
        self._flow_store.prune_events(flow_id, keep_last=keep_last)

    def _list_requested_work(self, flow_id: str) -> list[WorkRow]:
        return self._flow_store.list_requested_work(flow_id)

    def _get_work(self, flow_id: str, work_id: str) -> Optional[WorkRow]:
        return self._flow_store.get_work(flow_id, work_id)

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

    @staticmethod
    def _template_from_program(program: dict[str, Any]) -> str:
        return str(program.get("template") or "").strip()

    def _is_query_auto_profile(self, program: dict[str, Any]) -> bool:
        return self._profile_from_program(program) in BUILTIN_QUERY_AUTO_PROFILES

    @staticmethod
    def _template_id_from_value(value: str) -> str:
        template = value.strip()
        if not template:
            return ""
        if template.startswith(".template/"):
            return template
        if template.startswith("template/"):
            return "." + template
        return f".template/continuation/{template}"

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
                "template",
                "params",
                "frame_request",
                "decision_policy",
                "steps",
            )
        )

    def _program_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        program: dict[str, Any] = {}
        if "goal" in payload:
            program["goal"] = str(payload.get("goal") or "").strip().lower()
        if "profile" in payload:
            program["profile"] = str(payload.get("profile") or "").strip()
        if "template" in payload:
            program["template"] = str(payload.get("template") or "").strip()
        if "params" in payload:
            params = payload.get("params")
            program["params"] = params if isinstance(params, dict) else {}
        if "frame_request" in payload:
            frame_request = payload.get("frame_request")
            program["frame_request"] = frame_request if isinstance(frame_request, dict) else {}
        if "decision_policy" in payload:
            policy = payload.get("decision_policy")
            program["decision_policy"] = policy if isinstance(policy, dict) else {}
        if "steps" in payload:
            steps = payload.get("steps")
            program["steps"] = [dict(step) for step in steps if isinstance(step, dict)] if isinstance(steps, list) else []
        program["goal"] = self._goal_from_program(program)
        return program

    @staticmethod
    def _overrides_from_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], Optional[dict[str, str]]]:
        if "overrides" not in payload:
            return {}, None
        overrides = payload.get("overrides")
        if not isinstance(overrides, dict):
            return {}, {
                "code": "invalid_input",
                "message": "overrides must be an object",
            }
        unknown = sorted(str(key) for key in overrides.keys() if key not in PROGRAM_OVERRIDE_KEYS)
        if unknown:
            return {}, {
                "code": "invalid_input",
                "message": f"Unsupported overrides keys: {', '.join(unknown)}",
            }
        normalized: dict[str, Any] = {}
        if "params" in overrides:
            params = overrides.get("params")
            normalized["params"] = params if isinstance(params, dict) else {}
        if "frame_request" in overrides:
            frame_request = overrides.get("frame_request")
            normalized["frame_request"] = frame_request if isinstance(frame_request, dict) else {}
        if "decision_policy" in overrides:
            policy = overrides.get("decision_policy")
            normalized["decision_policy"] = policy if isinstance(policy, dict) else {}
        return normalized, None

    def _program_for_tick(self, base_program: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
        program = dict(base_program) if isinstance(base_program, dict) else {}
        if "params" in overrides:
            program["params"] = overrides["params"]
        if "frame_request" in overrides:
            program["frame_request"] = overrides["frame_request"]
        if "decision_policy" in overrides:
            program["decision_policy"] = overrides["decision_policy"]
        program["goal"] = self._goal_from_program(program)
        return program

    def _effective_frame_request(self, program: dict[str, Any]) -> dict[str, Any]:
        frame_request = program.get("frame_request")
        if isinstance(frame_request, dict):
            return frame_request

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
                ctx = self._env.get_context(
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
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        seed = frame_request.get("seed") or {}
        if not isinstance(seed, dict):
            return [], None

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
            item = self._env.get(str(value))
            if item is None:
                return [], None
            return [self._evidence_item(item, role="target", metadata_level=metadata_level)], None

        if mode == "query" and value:
            try:
                items = self._env.find(
                    query=str(value),
                    tags=where_tags or None,
                    limit=limit,
                    deep=deep,
                )
            except Exception as exc:
                return [], {
                    "code": "frame_evidence_error",
                    "message": f"query evidence retrieval failed: {exc}",
                }
            return ([
                self._evidence_item(it, role="candidate", metadata_level=metadata_level)
                for it in items[:limit]
            ], None)

        if mode == "similar_to" and value:
            try:
                items = self._env.find(
                    similar_to=str(value),
                    tags=where_tags or None,
                    limit=limit,
                    deep=deep,
                )
            except Exception as exc:
                return [], {
                    "code": "frame_evidence_error",
                    "message": f"similar_to evidence retrieval failed: {exc}",
                }
            return ([
                self._evidence_item(it, role="neighbor", metadata_level=metadata_level)
                for it in items[:limit]
            ], None)
        return [], None

    def _frame_evidence(
        self, program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        frame_request = self._effective_frame_request(program)
        return self._frame_evidence_for_request(frame_request)

    def _content_for_work(
        self, note_id: str, program: dict[str, Any], doc_coll: str,
    ) -> tuple[Optional[str], Optional[dict[str, str]]]:
        doc = self._env.get_document(note_id, collection=doc_coll)
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

    def _default_write_template_config(self) -> dict[str, Any]:
        return {
            "steps": [],
            "write_inline": {
                "op": "put_item",
                "fields": ["content", "uri", "id", "summary", "tags", "created_at", "force"],
                "require_exactly_one": [["content", "uri"]],
                "set": {
                    "queue_background_tasks": False,
                    "capture_write_context": True,
                },
            },
            "followups": [
                {
                    "task_type": "summarize",
                    "when": {"param_true": "processing.summarize"},
                    "content": "$write.content",
                    "tags": "$params.tags",
                },
                {
                    "task_type": "ocr",
                    "when": {"param_true": "processing.ocr"},
                    "content": "",
                    "metadata": {
                        "uri": "$write.uri",
                        "ocr_pages": "$write.ocr_pages",
                        "content_type": "$write.content_type",
                    },
                },
                {
                    "task_type": "analyze",
                    "when": {"param_true": "processing.analyze"},
                    "content": "",
                    "metadata": {
                        "tags": "$params.processing.analyze_tags",
                        "force": "$params.processing.analyze_force",
                    },
                },
                {
                    "task_type": "tag",
                    "when": {"param_true": "processing.tag"},
                    "content": "$write.content",
                    "metadata": {
                        "provider": "$params.processing.tag_provider",
                        "provider_params": "$params.processing.tag_provider_params",
                    },
                },
            ],
        }

    # -----------------------------------------------------------------------
    # State-doc integration
    # -----------------------------------------------------------------------

    def _load_state_doc(self, name: str) -> Optional[StateDoc]:
        """Load a compiled state doc from the store.

        State docs are keep notes at `.state/{name}` whose summary field
        contains the YAML body.  Returns None if the note does not exist or
        has no parseable YAML body.
        """
        note_id = f".state/{name}" if not name.startswith(".state/") else name
        doc_note = self._env.get(note_id)
        if doc_note is None:
            return None

        body = str(getattr(doc_note, "summary", "") or "").strip()
        if not body:
            return None

        try:
            return parse_state_doc(name, body)
        except (ValueError, RuntimeError) as exc:
            logger.warning("Failed to compile state doc %r: %s", note_id, exc)
            return None

    def _build_write_eval_context(
        self,
        target_id: str,
        program: dict[str, Any],
        write_context: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the CEL evaluation context for after-write state docs.

        Exposes the written item's full observable state so predicates can
        reference any field without the wiring layer pre-selecting which
        fields are available.

        Context namespaces:
            ``item.*``   — written item state (tags, content_length, source, …)
            ``params.*`` — caller-supplied write parameters
            ``write.*``  — write context (content_type, ocr_pages, …)
        """
        params = program.get("params") or {}
        if not isinstance(params, dict):
            params = {}

        content = write_context.get("content") or ""
        uri = write_context.get("uri") or ""
        ocr_pages = write_context.get("ocr_pages") or []

        # Fetch the written item to inspect current state
        item_note = self._env.get(target_id)
        item_tags: dict[str, Any] = {}
        item_summary = ""
        if item_note is not None:
            raw_tags = getattr(item_note, "tags", None)
            item_tags = dict(raw_tags) if isinstance(raw_tags, dict) else {}
            item_summary = str(getattr(item_note, "summary", "") or "")

        # has_summary: True only if the caller explicitly provided a summary.
        # The auto-truncated summary from put_item doesn't count — the purpose
        # of after-write is to decide whether AI summarization is needed.
        has_summary = bool(params.get("summary"))

        source = str(item_tags.get("_source") or "inline").strip().lower()
        is_system_note = target_id.startswith(".")

        item_ctx: dict[str, Any] = {
            "id": target_id,
            "content_length": len(content) if isinstance(content, str) else 0,
            "has_summary": has_summary,
            "is_system_note": is_system_note,
            "source": source,
            "has_uri": bool(uri),
            "uri": uri,
            "summary": item_summary,
            "tags": item_tags,
            "ocr_pages": list(ocr_pages) if isinstance(ocr_pages, list) else [],
        }

        params_ctx = dict(params)
        params_ctx["item_id"] = target_id

        write_ctx: dict[str, Any] = {
            "content_type": str(write_context.get("content_type") or ""),
            "uri": uri,
            "ocr_pages": item_ctx["ocr_pages"],
        }

        return {"item": item_ctx, "params": params_ctx, "write": write_ctx}

    def _state_doc_followup_ops(
        self,
        *,
        flow_id: str,
        doc: StateDoc,
        context: dict[str, Any],
        target_id: str,
        write_context: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[EvalResult], Optional[dict[str, str]]]:
        """Evaluate state doc and convert fired actions to enqueue_task ops.

        Returns ``(ops, eval_result, error)``.  The caller uses
        ``eval_result`` to inspect terminal/transition status from the
        state doc.  Action ``with:`` params are passed through generically
        as task metadata — the wiring layer does not filter or gate actions.
        """
        try:
            result = evaluate_state_doc(doc, context)
        except Exception as exc:
            logger.warning("State doc %r evaluation failed: %s", doc.name, exc)
            return [], None, {"code": "state_doc_eval_error", "message": str(exc)}

        if not result.actions:
            return [], result, None

        doc_coll = self._env.resolve_doc_collection()
        content = str(write_context.get("content") or "")
        item_tags = context.get("item", {}).get("tags")
        ops: list[dict[str, Any]] = []

        for action_entry in result.actions:
            action_name = str(action_entry.get("action") or "").strip()
            if not action_name:
                continue

            # State doc with: params become task metadata (generic passthrough)
            action_params = action_entry.get("params") or {}
            metadata = dict(action_params) if isinstance(action_params, dict) else {}

            # Merge write-context defaults that the task workflow may need.
            # State doc with: params take precedence over these defaults.
            write_defaults = {
                "uri": write_context.get("uri") or "",
                "content_type": write_context.get("content_type") or "",
            }
            ocr_pages = write_context.get("ocr_pages")
            if isinstance(ocr_pages, list):
                write_defaults["ocr_pages"] = self._coerce_ocr_pages(ocr_pages)
            for key, default_val in write_defaults.items():
                if key not in metadata and default_val:
                    metadata[key] = default_val

            op: dict[str, Any] = {
                "op": "enqueue_task",
                "task_type": action_name,
                "target": target_id,
                "collection": doc_coll,
                "content": content,
            }
            if metadata:
                op["metadata"] = metadata
            if isinstance(item_tags, dict):
                op["tags"] = dict(item_tags)

            ops.append(op)

        return ops, result, None

    @staticmethod
    def _parse_template_document(raw: str, *, template_id: str) -> tuple[Optional[dict[str, Any]], Optional[dict[str, str]]]:
        text = str(raw or "").strip()
        if not text:
            return None, {"code": "invalid_input", "message": f"Template is empty: {template_id}"}
        parsed: Any = None
        try:
            parsed = json.loads(text)
        except Exception:
            try:
                import yaml

                parsed = yaml.safe_load(text)
            except Exception:
                parsed = None
        if not isinstance(parsed, dict):
            return None, {"code": "invalid_input", "message": f"Template must parse to an object: {template_id}"}
        return {str(k): v for k, v in parsed.items()}, None

    @staticmethod
    def _merge_template_config(base: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in current.items():
            if key in {"steps", "followups"}:
                existing = merged.get(key)
                out: list[Any] = []
                if isinstance(existing, list):
                    out.extend(existing)
                if isinstance(value, list):
                    out.extend(value)
                merged[key] = out
                continue
            if key in {"write_inline", "decision_policy"}:
                if isinstance(merged.get(key), dict) and isinstance(value, dict):
                    combined = dict(merged.get(key) or {})
                    combined.update(value)
                    merged[key] = combined
                else:
                    merged[key] = value
                continue
            merged[key] = value
        return merged

    def _load_template_document(
        self,
        template_id: str,
        *,
        ancestry: Optional[list[str]] = None,
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, str]]]:
        ancestry = list(ancestry or [])
        if template_id in ancestry:
            chain = " -> ".join(ancestry + [template_id])
            return None, {"code": "invalid_input", "message": f"Template include cycle: {chain}"}

        doc = self._env.get(template_id)
        if doc is None:
            return None, None
        parsed, parse_err = self._parse_template_document(str(doc.summary or ""), template_id=template_id)
        if parse_err is not None:
            return None, parse_err
        assert isinstance(parsed, dict)

        include_refs = parsed.get("include")
        include_list: list[str] = []
        if include_refs is not None:
            if not isinstance(include_refs, list):
                return None, {"code": "invalid_input", "message": f"Template include must be a list: {template_id}"}
            for ref in include_refs:
                if not isinstance(ref, str) or not ref.strip():
                    return None, {"code": "invalid_input", "message": f"Template include entries must be non-empty strings: {template_id}"}
                include_list.append(ref.strip())

        merged: dict[str, Any] = {}
        for ref in include_list:
            child_id = self._template_id_from_value(ref)
            child, child_err = self._load_template_document(
                child_id,
                ancestry=ancestry + [template_id],
            )
            if child_err is not None:
                return None, child_err
            if child is None:
                return None, {"code": "unknown_template", "message": f"Template not found: {child_id}"}
            merged = self._merge_template_config(merged, child)

        current = dict(parsed)
        current.pop("include", None)
        merged = self._merge_template_config(merged, current)
        return merged, None

    def _load_template_config(
        self, program: dict[str, Any],
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, str]]]:
        if self._is_query_auto_profile(program):
            return None, None

        goal = self._goal_from_program(program)
        template_value = self._template_from_program(program)
        explicit_template = bool(template_value)

        if explicit_template:
            template_id = self._template_id_from_value(template_value)
        elif goal:
            template_id = f".template/continuation/{goal}"
        else:
            return None, None

        parsed, template_err = self._load_template_document(template_id)
        if parsed is None:
            if not explicit_template and template_id == ".template/continuation/write":
                # Bootstrap default for first write before system-doc migration creates
                # .template/continuation/write. Persisted system doc remains source of truth once present.
                return self._default_write_template_config(), None
            if template_err is not None:
                return None, template_err
            if explicit_template:
                return None, {"code": "unknown_template", "message": f"Template not found: {template_id}"}
            return None, None

        if not isinstance(program.get("decision_policy"), dict):
            template_policy = parsed.get("decision_policy")
            if isinstance(template_policy, dict):
                program["decision_policy"] = dict(template_policy)

        return {str(k): v for k, v in parsed.items()}, None

    def _template_steps(
        self, program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        parsed, template_err = self._load_template_config(program)
        if template_err is not None:
            return [], template_err
        if parsed is None:
            return [], None
        steps = parsed.get("steps")
        if steps is None:
            return [], None
        if not isinstance(steps, list):
            template_value = self._template_from_program(program)
            if template_value:
                template_id = self._template_id_from_value(template_value)
            else:
                template_id = f".template/continuation/{self._goal_from_program(program)}"
            return [], {"code": "invalid_input", "message": f"Template steps must be a list: {template_id}"}
        normalized_steps = [dict(step) for step in steps if isinstance(step, dict)]
        return normalized_steps, None

    def _resolved_plan(
        self, program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        inline = self._program_steps(program)
        if inline:
            return inline, None
        return self._template_steps(program)

    def _template_followups(
        self, program: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        parsed, template_err = self._load_template_config(program)
        if template_err is not None:
            return [], template_err
        if parsed is None:
            return [], None
        followups = parsed.get("followups")
        if followups is None:
            return [], None
        if not isinstance(followups, list):
            template_value = self._template_from_program(program)
            template_id = (
                self._template_id_from_value(template_value)
                if template_value
                else f".template/continuation/{self._goal_from_program(program)}"
            )
            return [], {"code": "invalid_input", "message": f"Template followups must be a list: {template_id}"}
        normalized = [dict(entry) for entry in followups if isinstance(entry, dict)]
        return normalized, None

    @staticmethod
    def _work_step_key(step: dict[str, Any], idx: int) -> str:
        kind = str(step.get("kind") or "").strip()
        if kind:
            return kind
        return f"step_{idx}"

    def _has_any_work_key(self, flow_id: str, key: str) -> bool:
        return self._flow_store.has_any_work_key(flow_id, key)

    def _has_completed_work_key(self, flow_id: str, key: str) -> bool:
        return self._flow_store.has_completed_work_key(flow_id, key)

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
        work_id = self._flow_store.insert_work(
            flow_id=flow_id,
            kind=kind,
            input_json=json.dumps(input_payload, ensure_ascii=False),
            output_contract_json=json.dumps(output_contract, ensure_ascii=False),
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
        output_contract = step.get("output_contract") or {}
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
            doc_coll = self._env.resolve_doc_collection()
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

        input_size_err = self._validate_json_size(
            input_payload,
            max_bytes=MAX_CONTINUE_WORK_INPUT_BYTES,
            label="work input payload",
        )
        if input_size_err is not None:
            return None, input_size_err

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
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        if not plan:
            return [], None

        created: list[dict[str, Any]] = []
        for idx, step in enumerate(plan):
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
    def _work_request_from_row(row: WorkRow) -> dict[str, Any]:
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
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        work_result_size_err = self._validate_json_size(
            work_result,
            max_bytes=MAX_CONTINUE_WORK_RESULT_BYTES,
            label="work_result",
        )
        if work_result_size_err is not None:
            return [], work_result_size_err

        work_id = str(work_result.get("work_id") or "")
        if not work_id:
            return [], {"code": "missing_required_field", "message": "work_result.work_id is required"}

        work = self._get_work(flow_id, work_id)
        if work is None:
            return [], {"code": "invalid_input", "message": f"Unknown work_id: {work_id}"}
        if work.status != "requested":
            return [], {"code": "invalid_input", "message": f"work_id is not pending: {work_id}"}

        status = str(work_result.get("status") or "ok")
        if status not in {"ok", "failed", "partial"}:
            return [], {"code": "invalid_input", "message": f"Invalid work_result.status: {status}"}

        if status != "ok":
            self._flow_store.update_work_result(
                work_id=work_id,
                status="failed",
                result_json=json.dumps(work_result, ensure_ascii=False),
            )
            return [
                self._applied_entry(
                    source="work",
                    work_id=work_id,
                    op="work_result",
                    target=None,
                    status="failed",
                )
            ], None

        work_input = json.loads(work.input_json)
        applied, err = self._apply_work_outputs(
            work_input,
            work_result,
            flow_id=flow_id,
            work_id=work_id,
        )
        if err is not None:
            return [], err
        self._flow_store.update_work_result(
            work_id=work_id,
            status="completed",
            result_json=json.dumps(work_result, ensure_ascii=False),
        )
        return applied, None

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
                ContinuationEngine._resolve_mutation_value(v, outputs=outputs, work_input=work_input)
                for v in value
            ]
        if isinstance(value, dict):
            return {
                str(k): ContinuationEngine._resolve_mutation_value(v, outputs=outputs, work_input=work_input)
                for k, v in value.items()
            }
        return value

    @staticmethod
    def _resolve_followup_ref(
        ref: str,
        *,
        program: dict[str, Any],
        write_context: dict[str, Any],
        target_id: str,
    ) -> Any:
        path = str(ref or "").strip()
        if path == "$target":
            return target_id
        if path.startswith("$params."):
            return ContinuationEngine._param_value(program, path[8:])
        if path == "$params":
            params = program.get("params")
            return dict(params) if isinstance(params, dict) else {}
        if path == "$write":
            return dict(write_context)
        if path.startswith("$write."):
            current: Any = write_context
            for part in path[7:].split("."):
                if not isinstance(current, dict):
                    return None
                current = current.get(part)
            return current
        return ref

    def _resolve_followup_value(
        self,
        value: Any,
        *,
        program: dict[str, Any],
        write_context: dict[str, Any],
        target_id: str,
    ) -> Any:
        if isinstance(value, str) and value.startswith("$"):
            return self._resolve_followup_ref(
                value,
                program=program,
                write_context=write_context,
                target_id=target_id,
            )
        if isinstance(value, list):
            return [
                self._resolve_followup_value(
                    item,
                    program=program,
                    write_context=write_context,
                    target_id=target_id,
                )
                for item in value
            ]
        if isinstance(value, dict):
            return {
                str(k): self._resolve_followup_value(
                    v,
                    program=program,
                    write_context=write_context,
                    target_id=target_id,
                )
                for k, v in value.items()
            }
        return value

    @staticmethod
    def _coerce_ocr_pages(value: Any) -> list[int]:
        if isinstance(value, list):
            out: list[int] = []
            for item in value:
                try:
                    out.append(int(item))
                except Exception:
                    continue
            return out
        return []

    def _followup_ops_from_template(
        self,
        *,
        flow_id: str,
        program: dict[str, Any],
        followups: list[dict[str, Any]],
        target_id: str,
        write_context: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        if not followups:
            return [], None

        ops: list[dict[str, Any]] = []
        doc_coll = self._env.resolve_doc_collection()
        for raw in followups:
            if not isinstance(raw, dict):
                continue
            when = raw.get("when")
            if not self._when_matches(when, flow_id=flow_id, program=program):
                continue
            task_type = str(raw.get("task_type") or "").strip()
            if not task_type:
                return [], {"code": "invalid_input", "message": "followup entry missing task_type"}

            resolved_content = self._resolve_followup_value(
                raw.get("content") or "",
                program=program,
                write_context=write_context,
                target_id=target_id,
            )
            content = "" if resolved_content is None else str(resolved_content)
            resolved_metadata = self._resolve_followup_value(
                raw.get("metadata") or {},
                program=program,
                write_context=write_context,
                target_id=target_id,
            )
            metadata = resolved_metadata if isinstance(resolved_metadata, dict) else {}
            resolved_tags = self._resolve_followup_value(
                raw.get("tags") or {},
                program=program,
                write_context=write_context,
                target_id=target_id,
            )
            tags = resolved_tags if isinstance(resolved_tags, dict) else {}

            if task_type == "summarize":
                max_len = self._param_value(program, "processing.max_summary_length")
                if isinstance(max_len, int) and len(content) <= max_len:
                    continue
                if not content:
                    continue

            if task_type == "ocr":
                metadata["ocr_pages"] = self._coerce_ocr_pages(metadata.get("ocr_pages"))
                if not metadata["ocr_pages"]:
                    continue
                if not metadata.get("uri"):
                    continue

            op: dict[str, Any] = {
                "op": "enqueue_task",
                "task_type": task_type,
                "target": target_id,
                "collection": doc_coll,
                "content": content,
            }
            if metadata:
                op["metadata"] = metadata
            if tags:
                op["tags"] = tags
            ops.append(op)
        return ops, None

    def _mutation_ops_from_work(
        self, work_input: dict[str, Any], outputs: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        raw_lists: list[list[Any]] = []

        output_mutations = outputs.get("mutations")
        if output_mutations is not None:
            if not isinstance(output_mutations, list):
                return [], {"code": "invalid_input", "message": "output.mutations must be a list"}
            raw_lists.append(output_mutations)

        apply_spec = work_input.get("apply") or {}
        if not isinstance(apply_spec, dict):
            return [], {"code": "invalid_input", "message": "work input apply spec must be an object"}
        apply_ops = apply_spec.get("ops")
        if apply_ops is not None:
            if not isinstance(apply_ops, list):
                return [], {"code": "invalid_input", "message": "apply.ops must be a list"}
            raw_lists.append(apply_ops)
        if not raw_lists:
            return [], None

        default_target = work_input.get("item_id")
        normalized: list[dict[str, Any]] = []
        for raw_ops in raw_lists:
            for raw in raw_ops:
                if not isinstance(raw, dict):
                    return [], {"code": "invalid_input", "message": "mutation op entries must be objects"}
                op = {str(k): v for k, v in raw.items()}
                op_name = str(op.get("op") or "")
                if not op_name:
                    return [], {"code": "invalid_input", "message": "mutation op requires op field"}
                if "target" not in op and default_target:
                    op["target"] = default_target

                resolved = {
                    str(k): self._resolve_mutation_value(v, outputs=outputs, work_input=work_input)
                    for k, v in op.items()
                }
                normalized.append(resolved)
        return normalized, None

    def _validate_mutation_op(
        self, op: dict[str, Any],
    ) -> tuple[dict[str, Any], Optional[dict[str, str]]]:
        allowed_fields: dict[str, set[str]] = {
            "upsert_item": {"op", "target", "content", "tags", "summary"},
            "put_item": {
                "op",
                "content",
                "uri",
                "id",
                "tags",
                "summary",
                "created_at",
                "force",
                "queue_background_tasks",
                "capture_write_context",
            },
            "set_tags": {"op", "target", "tags"},
            "set_summary": {"op", "target", "summary"},
            "enqueue_task": {"op", "task_type", "target", "collection", "content", "metadata", "tags"},
        }
        normalized = {str(k): v for k, v in op.items()}
        op_name = str(normalized.get("op") or "")
        allowed = allowed_fields.get(op_name)
        if allowed is None:
            return {}, {"code": "invalid_input", "message": f"Unsupported mutation op: {op_name}"}
        unknown_fields = sorted(str(key) for key in normalized.keys() if str(key) not in allowed)
        if unknown_fields:
            return {}, {
                "code": "invalid_input",
                "message": f"Unsupported fields for {op_name}: " + ", ".join(unknown_fields),
            }
        target = normalized.get("target")
        if target and op_name in {"upsert_item", "set_tags", "set_summary"}:
            target_id = str(target)
            if self._is_protected_note_id(target_id):
                return {}, {
                    "code": "forbidden_target",
                    "message": f"Mutation target is protected: {target_id}",
                }
        if op_name == "upsert_item":
            if not target:
                return {}, {"code": "missing_required_field", "message": "upsert_item requires target"}
            if normalized.get("content") is None:
                return {}, {"code": "missing_required_field", "message": "upsert_item requires content"}
            tags = normalized.get("tags")
            if tags is not None and not isinstance(tags, dict):
                return {}, {"code": "invalid_input", "message": "upsert_item.tags must be an object"}
        elif op_name == "put_item":
            has_content = normalized.get("content") is not None
            has_uri = bool(normalized.get("uri"))
            if has_content == has_uri:
                return {}, {
                    "code": "missing_required_field",
                    "message": "put_item requires exactly one of content or uri",
                }
            tags = normalized.get("tags")
            if tags is not None and not isinstance(tags, dict):
                return {}, {"code": "invalid_input", "message": "put_item.tags must be an object"}
            if "force" in normalized and not isinstance(normalized.get("force"), bool):
                return {}, {"code": "invalid_input", "message": "put_item.force must be a boolean"}
            if "queue_background_tasks" in normalized and not isinstance(normalized.get("queue_background_tasks"), bool):
                return {}, {"code": "invalid_input", "message": "put_item.queue_background_tasks must be a boolean"}
            if "capture_write_context" in normalized and not isinstance(normalized.get("capture_write_context"), bool):
                return {}, {"code": "invalid_input", "message": "put_item.capture_write_context must be a boolean"}
        elif op_name == "set_tags":
            if not target:
                return {}, {"code": "missing_required_field", "message": "set_tags requires target"}
            if not isinstance(normalized.get("tags"), dict):
                return {}, {"code": "missing_required_field", "message": "set_tags requires tags object"}
        elif op_name == "set_summary":
            if not target:
                return {}, {"code": "missing_required_field", "message": "set_summary requires target"}
            if normalized.get("summary") is None:
                return {}, {"code": "missing_required_field", "message": "set_summary requires summary"}
        elif op_name == "enqueue_task":
            if not target:
                return {}, {"code": "missing_required_field", "message": "enqueue_task requires target"}
            if not str(normalized.get("task_type") or "").strip():
                return {}, {"code": "missing_required_field", "message": "enqueue_task requires task_type"}
            if "metadata" in normalized and not isinstance(normalized.get("metadata"), dict):
                return {}, {"code": "invalid_input", "message": "enqueue_task.metadata must be an object"}
            if "tags" in normalized and not isinstance(normalized.get("tags"), dict):
                return {}, {"code": "invalid_input", "message": "enqueue_task.tags must be an object"}
        return normalized, None

    def _insert_pending_mutation(
        self, *, flow_id: str, work_id: Optional[str], op: dict[str, Any],
    ) -> str:
        op_json = json.dumps(op, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return self._flow_store.insert_pending_mutation(
            flow_id=flow_id,
            work_id=work_id,
            op_json=op_json,
        )

    def _get_mutation(self, mutation_id: str) -> Optional[MutationRow]:
        return self._flow_store.get_mutation(mutation_id)

    def _set_mutation_status(self, mutation_id: str, *, status: str, error: Optional[str] = None) -> None:
        self._flow_store.set_mutation_status(
            mutation_id,
            status=status,
            error=error,
        )

    def _list_pending_mutations(
        self, *, flow_id: Optional[str] = None, limit: int = 100,
    ) -> list[MutationRow]:
        return self._flow_store.list_pending_mutations(flow_id=flow_id, limit=limit)

    def _apply_single_mutation(
        self, op: dict[str, Any],
    ) -> tuple[dict[str, Any], Optional[dict[str, str]]]:
        op_name = str(op.get("op") or "")
        target = str(op.get("target") or "")
        if op_name == "upsert_item":
            content = op.get("content")
            tags = op.get("tags")
            summary = op.get("summary")
            self._env.upsert_item(
                target=target,
                content=str(content),
                tags=tags if isinstance(tags, dict) else None,
                summary=str(summary) if summary is not None else None,
            )
            return {"op": op_name, "target": target, "status": "applied"}, None
        if op_name == "put_item":
            tags = op.get("tags")
            summary = op.get("summary")
            item = self._env.put_item(
                content=str(op.get("content")) if op.get("content") is not None else None,
                uri=str(op.get("uri")) if op.get("uri") else None,
                id=str(op.get("id")) if op.get("id") else None,
                summary=str(summary) if summary is not None else None,
                tags=tags if isinstance(tags, dict) else None,
                created_at=str(op.get("created_at")) if op.get("created_at") else None,
                force=bool(op.get("force", False)),
                queue_background_tasks=bool(op.get("queue_background_tasks", True)),
                capture_write_context=bool(op.get("capture_write_context", False)),
            )
            changed = getattr(item, "changed", None)
            status = "noop" if changed is False else "applied"
            return {
                "op": op_name,
                "target": str(getattr(item, "id", "") or op.get("id") or ""),
                "status": status,
            }, None
        if op_name == "set_tags":
            tags = op.get("tags")
            if not isinstance(tags, dict):
                return {}, {"code": "missing_required_field", "message": "set_tags requires tags object"}
            normalized_tags = {str(k): v for k, v in tags.items()}
            self._env.set_tags(target, normalized_tags)
            return {"op": op_name, "target": target, "status": "applied"}, None
        if op_name == "set_summary":
            summary = op.get("summary")
            existing = self._env.get(target)
            if existing is None:
                return {}, {"code": "invalid_input", "message": f"Target note not found: {target}"}
            if existing.summary == str(summary):
                return {"op": op_name, "target": target, "status": "noop"}, None
            self._env.set_summary(target, str(summary))
            return {"op": op_name, "target": target, "status": "applied"}, None
        if op_name == "enqueue_task":
            task_type = str(op.get("task_type") or "").strip()
            metadata = op.get("metadata") if isinstance(op.get("metadata"), dict) else {}
            tags = op.get("tags") if isinstance(op.get("tags"), dict) else {}
            collection = str(op.get("collection") or self._env.resolve_doc_collection())
            content = str(op.get("content") or "")
            self._env.enqueue_task(
                task_type=task_type,
                item_id=target,
                collection=collection,
                content=content,
                metadata=metadata,
                tags=tags,
            )
            return {
                "op": op_name,
                "target": target,
                "status": "queued",
                "task_type": task_type,
            }, None
        return {}, {"code": "invalid_input", "message": f"Unsupported mutation op: {op_name}"}

    def _replay_pending_mutations(
        self, *, flow_id: Optional[str] = None, limit: int = 100,
    ) -> list[dict[str, str]]:
        pending = self._list_pending_mutations(flow_id=flow_id, limit=limit)
        failures: list[dict[str, str]] = []
        for row in pending:
            try:
                op = json.loads(row.op_json)
            except Exception as exc:
                msg = f"invalid op_json: {exc}"
                self._set_mutation_status(row.mutation_id, status="failed", error=msg)
                failures.append({
                    "code": "mutation_apply_failed",
                    "message": f"mutation {row.mutation_id} failed: {msg}",
                })
                continue
            applied, err = self._apply_single_mutation(op)
            if err is not None:
                msg = f"{err.get('code')}: {err.get('message')}"
                self._set_mutation_status(
                    row.mutation_id,
                    status="failed",
                    error=msg,
                )
                failures.append({
                    "code": "mutation_apply_failed",
                    "message": f"mutation {row.mutation_id} failed: {msg}",
                })
                continue
            self._set_mutation_status(row.mutation_id, status="applied")
            logger.debug(
                "Replayed pending mutation %s flow=%s op=%s target=%s",
                row.mutation_id,
                row.flow_id,
                applied.get("op"),
                applied.get("target"),
            )
        return failures

    def _apply_mutation_ops(
        self, ops: list[dict[str, Any]], *, flow_id: str, work_id: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        applied: list[dict[str, Any]] = []
        source = "work" if work_id else "inline"
        in_tx = self._flow_store.in_transaction()
        for raw_op in ops:
            if not isinstance(raw_op, dict):
                return [], {"code": "invalid_input", "message": "mutation op entries must be objects"}
            op, err = self._validate_mutation_op(raw_op)
            if err is not None:
                return [], err
            target_hint = str(op.get("target") or op.get("id") or "")
            mutation_id = self._insert_pending_mutation(flow_id=flow_id, work_id=work_id, op=op)
            existing = self._get_mutation(mutation_id)
            if existing is not None and existing.status == "applied":
                applied.append(self._applied_entry(
                    source=source,
                    work_id=work_id,
                    op=str(op.get("op") or ""),
                    target=target_hint,
                    status="applied",
                    mutation_id=mutation_id,
                ))
                continue
            if existing is not None and existing.status == "failed":
                return applied, {
                    "code": "mutation_failed",
                    "message": existing.error or "previous mutation attempt failed",
                }
            apply_in_tx = str(op.get("op") or "") in {"put_item", "enqueue_task"}
            if in_tx and not apply_in_tx:
                applied.append(self._applied_entry(
                    source=source,
                    work_id=work_id,
                    op=str(op.get("op") or ""),
                    target=target_hint,
                    status="queued",
                    mutation_id=mutation_id,
                ))
                continue
            applied_entry, apply_err = self._apply_single_mutation(op)
            if apply_err is not None:
                self._set_mutation_status(
                    mutation_id,
                    status="failed",
                    error=f"{apply_err.get('code')}: {apply_err.get('message')}",
                )
                return applied, apply_err
            self._set_mutation_status(mutation_id, status="applied")
            applied.append(self._applied_entry(
                source=source,
                work_id=work_id,
                op=str(applied_entry.get("op") or ""),
                target=str(applied_entry.get("target") or ""),
                status=str(applied_entry.get("status") or "applied"),
                mutation_id=mutation_id,
            ))
        return applied, None

    def _apply_work_outputs(
        self,
        work_input: dict[str, Any],
        work_result: dict[str, Any],
        *,
        flow_id: str,
        work_id: Optional[str],
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        outputs = work_result.get("outputs") or {}
        if not isinstance(outputs, dict):
            return [], {"code": "invalid_input", "message": "work_result.outputs must be an object"}
        ops, err = self._mutation_ops_from_work(work_input, outputs)
        if err is not None:
            return [], err
        if not ops:
            return [], None
        applied, err = self._apply_mutation_ops(ops, flow_id=flow_id, work_id=work_id)
        if err is not None:
            return [], err
        return applied, None

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
        discriminators: Optional[dict[str, Any]] = None,
        include_debug_fields: bool = False,
    ) -> dict[str, Any]:
        params = program.get("params") if isinstance(program.get("params"), dict) else {}
        query_text = str(params.get("text") or "")

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
        }
        frame: dict[str, Any] = {
            "evidence": evidence,
            "decision": (
                discriminators
                if isinstance(discriminators, dict)
                else self._empty_discriminators()
            ),
        }
        if include_debug_fields:
            frame["debug"] = {
                "slots": slots,
                "task": task,
                "hygiene": [],
                "budget_used": {
                    "tokens": self._as_int((budget_used or {}).get("tokens"), 0),
                    "nodes": self._as_int((budget_used or {}).get("nodes"), 0),
                },
                "status": status,
            }
        return frame

    @staticmethod
    def _empty_discriminators() -> dict[str, Any]:
        return ContinuationDecisionPolicy.empty_discriminators()

    @staticmethod
    def _as_float(value: Any, default: float) -> float:
        return ContinuationDecisionPolicy.as_float(value, default)

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
        return ContinuationDecisionPolicy.clamp(value, low, high)

    def _decision_policy_from_program(self, program: dict[str, Any]) -> dict[str, Any]:
        return self._decision_policy.decision_policy_from_program(program)

    @staticmethod
    def _empty_lineage_signal() -> dict[str, Any]:
        return ContinuationDecisionPolicy.empty_lineage_signal()

    def _lineage_signal(
        self, evidence: list[dict[str, Any]], *, field: str, topk: int,
    ) -> dict[str, Any]:
        return self._decision_policy.lineage_signal(evidence, field=field, topk=topk)

    def _lineage_signals(self, evidence: list[dict[str, Any]], *, topk: int) -> dict[str, Any]:
        return self._decision_policy.lineage_signals(evidence, topk=topk)

    def _decision_override_from_payload(
        self, payload: dict[str, Any],
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, str]]]:
        return ContinuationDecisionPolicy.decision_override_from_payload(payload)

    def _candidate_subject_keys(
        self, frame_request: dict[str, Any], evidence: list[dict[str, Any]],
    ) -> list[str]:
        return self._decision_policy.candidate_subject_keys(frame_request, evidence)

    def _tag_key_kind(self, key: str, *, cache: Optional[dict[str, str]] = None) -> str:
        return self._decision_policy.tag_key_kind(key, cache=cache)

    @staticmethod
    def _best_fact_from_counts(
        counts: dict[tuple[str, str], int], *, total: int,
    ) -> Optional[str]:
        return ContinuationDecisionPolicy.best_fact_from_counts(counts, total=total)

    def _tag_profile(self, evidence: list[dict[str, Any]], *, topk: int) -> dict[str, Any]:
        return self._decision_policy.tag_profile(evidence, topk=topk)

    def _query_stats(
        self,
        *,
        frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
        previous_evidence_ids: list[str],
        topk: int,
    ) -> dict[str, float]:
        return self._decision_policy.query_stats(
            frame_request=frame_request,
            evidence=evidence,
            previous_evidence_ids=previous_evidence_ids,
            topk=topk,
        )

    def _choose_strategy(
        self,
        *,
        query_stats: dict[str, float],
        lineage: dict[str, Any],
        policy: dict[str, Any],
        staleness: dict[str, Any],
        decision_override: Optional[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        return self._decision_policy.choose_strategy(
            query_stats=query_stats,
            lineage=lineage,
            policy=policy,
            staleness=staleness,
            decision_override=decision_override,
        )

    @staticmethod
    def _pivot_ids(evidence: list[dict[str, Any]], *, strategy: str, max_pivots: int) -> list[str]:
        return ContinuationDecisionPolicy.pivot_ids(evidence, strategy=strategy, max_pivots=max_pivots)

    def _decision_capsule(
        self,
        *,
        program: dict[str, Any],
        frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
        previous_evidence_ids: list[str],
        decision_override: Optional[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return self._decision_policy.decision_capsule(
            program=program,
            frame_request=frame_request,
            evidence=evidence,
            previous_evidence_ids=previous_evidence_ids,
            decision_override=decision_override,
        )

    def _dominant_tag_fact(self, evidence: list[dict[str, Any]]) -> Optional[str]:
        return self._decision_policy.dominant_tag_fact(evidence)

    def _query_auto_next_frame_request(
        self,
        *,
        current_frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
        decision_snapshot: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        return self._decision_policy.query_auto_next_frame_request(
            current_frame_request=current_frame_request,
            evidence=evidence,
            decision_snapshot=decision_snapshot,
        )

    def _top_tag_facts(
        self, evidence: list[dict[str, Any]], *, max_facts: int = 2,
    ) -> list[str]:
        return self._decision_policy.top_tag_facts(evidence, max_facts=max_facts)

    @staticmethod
    def _query_auto_branch_utility(
        *, discriminators: dict[str, Any], evidence_count: int,
    ) -> float:
        return ContinuationDecisionPolicy.query_auto_branch_utility(
            discriminators=discriminators,
            evidence_count=evidence_count,
        )

    def _query_auto_top2_plus_bridge_branches(
        self,
        *,
        current_frame_request: dict[str, Any],
        evidence: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        return self._decision_policy.query_auto_top2_plus_bridge_branches(
            current_frame_request=current_frame_request,
            evidence=evidence,
        )

    def _apply_inline_write(
        self, state: dict[str, Any], program: dict[str, Any], *, flow_id: str,
    ) -> tuple[list[dict[str, Any]], Optional[dict[str, str]]]:
        if state.get("frontier", {}).get("write_applied"):
            return [], None

        if self._goal_from_program(program) != "write":
            return [], None
        params = program.get("params") or {}
        if not isinstance(params, dict):
            return [], {"code": "invalid_input", "message": "write goal requires params object"}

        parsed_template, template_err = self._load_template_config(program)
        if template_err is not None:
            return [], template_err
        write_inline = parsed_template.get("write_inline") if isinstance(parsed_template, dict) else None
        if write_inline is not None and not isinstance(write_inline, dict):
            return [], {"code": "invalid_input", "message": "template.write_inline must be an object"}

        op_name = "put_item"
        op_fields = ["content", "uri", "id", "summary", "tags", "created_at", "force"]
        require_exactly_one: list[list[str]] = [["content", "uri"]]
        inline_set: dict[str, Any] = {}

        if isinstance(write_inline, dict):
            op_name = str(write_inline.get("op") or op_name).strip()
            fields = write_inline.get("fields")
            if fields is not None:
                if not isinstance(fields, list) or not all(isinstance(field, str) and field.strip() for field in fields):
                    return [], {"code": "invalid_input", "message": "template.write_inline.fields must be a string list"}
                op_fields = [str(field).strip() for field in fields]
            rules = write_inline.get("require_exactly_one")
            if rules is not None:
                normalized_rules: list[list[str]] = []
                if isinstance(rules, list):
                    for rule in rules:
                        if isinstance(rule, str) and rule.strip():
                            normalized_rules.append([str(rule).strip()])
                        elif isinstance(rule, list):
                            group = [str(key).strip() for key in rule if isinstance(key, str) and str(key).strip()]
                            if group:
                                normalized_rules.append(group)
                        else:
                            return [], {"code": "invalid_input", "message": "template.write_inline.require_exactly_one entries must be strings or string lists"}
                else:
                    return [], {"code": "invalid_input", "message": "template.write_inline.require_exactly_one must be a list"}
                require_exactly_one = normalized_rules
            static_fields = write_inline.get("set")
            if static_fields is not None:
                if not isinstance(static_fields, dict):
                    return [], {"code": "invalid_input", "message": "template.write_inline.set must be an object"}
                inline_set = {str(k): v for k, v in static_fields.items()}

        def _present(key: str) -> bool:
            if key == "content":
                return params.get("content") is not None
            return bool(params.get(key))

        for group in require_exactly_one:
            present = sum(1 for key in group if _present(key))
            if present != 1:
                joined = ", ".join(f"params.{k}" for k in group)
                return [], {
                    "code": "missing_required_field",
                    "message": f"write goal requires exactly one of {joined}",
                }

        op: dict[str, Any] = {"op": op_name}
        for key in op_fields:
            if key in params:
                op[key] = params.get(key)
        if inline_set:
            op.update(inline_set)
        ops = [op]
        applied, err = self._apply_mutation_ops(ops, flow_id=flow_id, work_id=None)
        if err is not None:
            return [], err

        target_id = ""
        write_status = ""
        for entry in reversed(applied):
            if not isinstance(entry, dict):
                continue
            if str(entry.get("op") or "") not in {"put_item", "upsert_item"}:
                continue
            target_id = str(entry.get("target") or "").strip()
            write_status = str(entry.get("status") or "").strip()
            if target_id:
                break
        if not target_id:
            if params.get("id"):
                target_id = str(params.get("id"))
            elif params.get("uri"):
                target_id = str(params.get("uri"))

        if target_id and write_status != "noop":
            write_context = self._env.consume_write_context(target_id) or {}
            if not isinstance(write_context, dict):
                write_context = {}
            if "content" not in write_context and params.get("content") is not None:
                write_context["content"] = params.get("content")
            if "uri" not in write_context and params.get("uri"):
                write_context["uri"] = params.get("uri")
            if "ocr_pages" not in write_context:
                write_context["ocr_pages"] = []
            if "content_type" not in write_context:
                write_context["content_type"] = ""

            # Try state-doc evaluation first; fall back to template followups.
            # Skip state-doc evaluation for system notes (id starts with "."):
            # these are infrastructure and should not trigger user-defined
            # processing flows.
            followup_ops: list[dict[str, Any]] = []
            state_doc = None
            if not target_id.startswith("."):
                state_doc = self._load_state_doc("after-write")
            if state_doc is not None:
                eval_ctx = self._build_write_eval_context(target_id, program, write_context)
                followup_ops, eval_result, sd_err = self._state_doc_followup_ops(
                    flow_id=flow_id,
                    doc=state_doc,
                    context=eval_ctx,
                    target_id=target_id,
                    write_context=write_context,
                )
                if sd_err is not None:
                    logger.warning(
                        "State doc after-write eval failed, falling back to template: %s",
                        sd_err.get("message"),
                    )
                    followup_ops = []
                    state_doc = None  # trigger fallback
                elif eval_result is not None:
                    if eval_result.terminal == "error":
                        return applied, {
                            "code": "state_doc_terminal_error",
                            "message": (
                                eval_result.terminal_data.get("reason", "state doc returned error")
                                if isinstance(eval_result.terminal_data, dict)
                                else "state doc returned error"
                            ),
                        }
                    if eval_result.transition is not None:
                        # Record transition for the flow to act on
                        frontier = state.setdefault("frontier", {})
                        frontier["state_doc_transition"] = eval_result.transition

            if state_doc is None:
                followups, followup_err = self._template_followups(program)
                if followup_err is not None:
                    return [], followup_err
                if followups:
                    followup_ops, followup_build_err = self._followup_ops_from_template(
                        flow_id=flow_id,
                        program=program,
                        followups=followups,
                        target_id=target_id,
                        write_context=write_context,
                    )
                    if followup_build_err is not None:
                        return [], followup_build_err

            if followup_ops:
                followup_applied, followup_apply_err = self._apply_mutation_ops(
                    followup_ops,
                    flow_id=flow_id,
                    work_id=None,
                )
                if followup_apply_err is not None:
                    return [], followup_apply_err
                applied.extend(followup_applied)
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

    def _emit_pause_decision_request(
        self,
        *,
        flow_id: str,
        cursor: str,
        goal: str,
        stage: str,
        requested_work: list[dict[str, Any]],
    ) -> Optional[str]:
        requested_kinds = sorted(
            {
                str(w.get("kind") or "").strip()
                for w in requested_work
                if isinstance(w, dict) and str(w.get("kind") or "").strip()
            }
        )
        event_material = f"{flow_id}|paused|{goal}"
        event_id = hashlib.sha256(event_material.encode("utf-8")).hexdigest()[:16]
        context = {
            "event_id": event_id,
            "flow_id": flow_id,
            "cursor": cursor,
            "goal": goal or "unknown",
            "stage": stage or "paused",
            "requested_count": len(requested_work),
            "requested_kinds": ",".join(requested_kinds) if requested_kinds else "none",
            "reason": "Flow paused awaiting work feedback or decision",
        }
        try:
            return self._env.emit_escalation_note(
                "decision-request",
                context=context,
            )
        except Exception as exc:
            logger.warning("Could not emit pause decision request for %s: %s", flow_id, exc)
            return None

    def continue_flow(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("continue input must be a JSON object")
        legacy_keys = [key for key in ("flow_id", "state_version", "feedback") if key in payload]
        if legacy_keys:
            raise ValueError(f"Unsupported fields in continue payload: {', '.join(legacy_keys)}")
        include_debug_fields = self._include_debug_fields(payload)
        payload_size_err = self._validate_json_size(
            payload,
            max_bytes=MAX_CONTINUE_PAYLOAD_BYTES,
            label="continue payload",
        )
        if payload_size_err is not None:
            raise ValueError(payload_size_err["message"])

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

            replay_failures = self._replay_pending_mutations(limit=100)
            if replay_failures:
                logger.warning(
                    "Continuation replay found %d pending mutation failures before tick",
                    len(replay_failures),
                )

            self._flow_store.begin_immediate()
            try:
                cursor_token = payload.get("cursor")
                decoded_cursor = self._decode_cursor(cursor_token) if cursor_token is not None else None
                if cursor_token is not None and decoded_cursor is None:
                    raise ValueError("invalid cursor")
                is_new_flow = decoded_cursor is None
                if decoded_cursor is None:
                    flow = self._create_flow()
                    expected_version = None
                else:
                    flow_id, expected_version = decoded_cursor
                    flow = self._get_flow(str(flow_id))
                    if flow is None:
                        raise ValueError("unknown cursor")

                if flow is None:
                    raise ValueError("flow state unavailable")
                if expected_version is not None and int(expected_version) != flow.state_version:
                    if self._flow_store.in_transaction():
                        self._flow_store.rollback()
                    state = json.loads(flow.state_json)
                    program = state.get("program") or {}
                    if not isinstance(program, dict):
                        program = {}
                    goal = self._goal_from_program(program)
                    note_id = self._infer_note_id(program) if program else None
                    evidence, _ = self._frame_evidence(program) if program else ([], None)
                    conflict_output = {
                        "request_id": request_id,
                        "cursor": self._encode_cursor(flow.flow_id, flow.state_version),
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
                            include_debug_fields=include_debug_fields,
                        ),
                        "work": [],
                        "applied_ops": [],
                        "errors": [{
                            "code": "state_conflict",
                            "message": "cursor is not current for this flow",
                        }],
                    }
                    if include_debug_fields:
                        conflict_output["state"] = state
                        conflict_output["output_hash"] = self._output_hash(
                            {
                                "cursor": conflict_output["cursor"],
                                "status": "failed",
                                "work": [],
                                "errors": conflict_output["errors"],
                            }
                        )
                    return conflict_output

                state = json.loads(flow.state_json)
                errors: list[dict[str, str]] = []
                applied_ops: list[dict[str, Any]] = []
                resolved_plan: list[dict[str, Any]] = []
                decision_discriminators = self._empty_discriminators()
                decision_snapshot: Optional[dict[str, Any]] = None
                used_auto_query_pending = False
                used_auto_query_branch_id: Optional[str] = None
                overrides, overrides_err = self._overrides_from_payload(payload)
                base_program = state.get("program") if isinstance(state.get("program"), dict) else {}
                if is_new_flow:
                    base_program = self._program_from_payload(payload) if self._program_has_inputs(payload) else {}
                elif self._program_has_inputs(payload):
                    errors.append({
                        "code": "invalid_input",
                        "message": "Flow program is immutable after start; use overrides",
                    })
                program = self._program_for_tick(base_program, overrides)
                decision_override, decision_override_err = self._decision_override_from_payload(payload)
                if overrides_err is not None:
                    errors.append(overrides_err)
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
                if (
                    isinstance(frontier_state, dict)
                    and (
                        "frame_request" in payload
                        or ("frame_request" in overrides)
                    )
                ):
                    frontier_state.pop("auto_query_next_frame_request", None)
                    frontier_state.pop("auto_query_branch_plan", None)
                    frontier_state.pop("auto_query_selected_branch", None)
                    frontier_state.pop("auto_query_refined", None)
                if is_new_flow and not self._program_has_inputs(payload):
                    errors.append({
                        "code": "missing_required_field",
                        "message": "New flow requires top-level flow fields",
                    })
                if not program:
                    errors.append({
                        "code": "missing_required_field",
                        "message": "No flow program available",
                    })
                else:
                    program["goal"] = self._goal_from_program(program)
                    resolved_plan, plan_err = self._resolved_plan(program)
                    if plan_err is not None:
                        errors.append(plan_err)
                    frame_request = self._effective_frame_request(program)
                    program["frame_request"] = frame_request
                    frame_err = self._validate_frame_request(frame_request)
                    if frame_err is not None:
                        errors.append(frame_err)

                if "feedback" in payload:
                    errors.append({
                        "code": "invalid_input",
                        "message": "feedback is not supported; use top-level work_results",
                    })
                work_results = payload.get("work_results") or []
                if not isinstance(work_results, list):
                    errors.append({
                        "code": "invalid_input",
                        "message": "work_results must be a list",
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
                        applied_ops.extend(applied)

                requested = self._list_requested_work(flow.flow_id)
                request_work: list[dict[str, Any]] = []
                goal = self._goal_from_program(program) if program else ""
                note_id = self._infer_note_id(program) if program else None
                previous_evidence_ids = state.get("frontier", {}).get("evidence_ids") or []
                if not isinstance(previous_evidence_ids, list):
                    previous_evidence_ids = []
                previous_dependency_stamp = state.get("frontier", {}).get("dependency_stamp")
                if not isinstance(previous_dependency_stamp, str):
                    previous_dependency_stamp = ""
                evidence: list[dict[str, Any]] = []
                if program and not errors:
                    evidence, evidence_err = self._frame_evidence(program)
                    if evidence_err is not None:
                        errors.append(evidence_err)
                if program and not errors:
                    write_ops, write_err = self._apply_inline_write(
                        state,
                        program,
                        flow_id=flow.flow_id,
                    )
                    if write_err is not None:
                        errors.append(write_err)
                    elif write_ops:
                        applied_ops.extend(write_ops)
                if program and not errors:
                    frame_request = self._effective_frame_request(program)
                    dependency_stamp = self._dependency_stamp(
                        frame_request=frame_request,
                        evidence=evidence,
                    )
                    can_shallow_verify = (
                        not self._is_query_auto_profile(program)
                        and not self._program_has_inputs(payload)
                        and not bool(overrides)
                        and not bool(work_results)
                        and not bool(requested)
                        and not bool(applied_ops)
                        and bool(previous_dependency_stamp)
                        and previous_dependency_stamp == dependency_stamp
                    )
                    if can_shallow_verify:
                        logger.debug(
                            "Shallow verify reuse for flow %s dependency stamp %s",
                            flow.flow_id,
                            dependency_stamp[:10],
                        )
                    else:
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
                cursor_stage = stage

                next_step = int((state.get("cursor") or {}).get("step", 0)) + 1
                state["cursor"] = {"step": next_step, "stage": cursor_stage, "phase": stage}
                state["program"] = base_program
                frontier = state.setdefault("frontier", {})
                frontier["evidence_ids"] = [row.get("id") for row in evidence]
                if program:
                    frontier["dependency_stamp"] = self._dependency_stamp(
                        frame_request=self._effective_frame_request(program),
                        evidence=evidence,
                    )
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
                    status = "in_progress"

                frame = self._build_frame(
                    flow.flow_id,
                    note_id,
                    status,
                    goal,
                    cursor_stage,
                    program,
                    evidence,
                    budget_used,
                    decision_discriminators,
                    include_debug_fields=include_debug_fields,
                )

                new_state_version = flow.state_version + 1
                state_json = json.dumps(state, ensure_ascii=False)
                state_bytes = len(state_json.encode("utf-8"))
                if state_bytes > MAX_CONTINUE_STATE_BYTES:
                    raise ValueError(
                        f"continuation state exceeds max size ({state_bytes} > {MAX_CONTINUE_STATE_BYTES} bytes)"
                    )
                self._flow_store.update_flow(
                    flow.flow_id,
                    state_version=new_state_version,
                    status=status,
                    state_json=state_json,
                )
                event_payload = {
                    "status": status,
                    "applied": applied_ops,
                    "errors": errors,
                    "request_id": request_id,
                    "goal": goal,
                }
                self._flow_store.insert_event(
                    flow_id=flow.flow_id,
                    event_type="tick",
                    payload_json=json.dumps(event_payload, ensure_ascii=False),
                )
                self._prune_events(flow.flow_id)

                output = {
                    "request_id": request_id,
                    "cursor": self._encode_cursor(flow.flow_id, new_state_version),
                    "status": status,
                    "frame": frame,
                    "work": request_work,
                    "applied_ops": applied_ops,
                    "errors": errors,
                }
                if include_debug_fields:
                    output["state"] = state
                    output["output_hash"] = self._output_hash(
                        {
                            "cursor": output["cursor"],
                            "status": status,
                            "work": request_work,
                            "errors": errors,
                        }
                    )
                self._flow_store.commit()
                post_failures = self._replay_pending_mutations(flow_id=flow.flow_id, limit=200)
                if post_failures:
                    self._flow_store.begin_immediate()
                    try:
                        latest = self._get_flow(flow.flow_id)
                        if latest is not None:
                            latest_state = json.loads(latest.state_json)
                            cursor = latest_state.get("cursor")
                            if isinstance(cursor, dict):
                                cursor["stage"] = "failed"
                                cursor["phase"] = "failed"
                            corrected_errors = list(errors) + post_failures
                            corrected_status = "failed"
                            corrected_version = latest.state_version + 1
                            corrected_state_json = json.dumps(latest_state, ensure_ascii=False)
                            self._flow_store.update_flow(
                                flow.flow_id,
                                state_version=corrected_version,
                                status=corrected_status,
                                state_json=corrected_state_json,
                            )
                            self._flow_store.insert_event(
                                flow_id=flow.flow_id,
                                event_type="mutation_failure",
                                payload_json=json.dumps(
                                    {
                                        "status": corrected_status,
                                        "errors": corrected_errors,
                                        "request_id": request_id,
                                        "goal": goal,
                                    },
                                    ensure_ascii=False,
                                ),
                            )
                            self._prune_events(flow.flow_id)
                            self._flow_store.commit()
                            output["status"] = corrected_status
                            output["cursor"] = self._encode_cursor(flow.flow_id, corrected_version)
                            output["errors"] = corrected_errors
                            if include_debug_fields:
                                output["state"] = latest_state
                        else:
                            self._flow_store.rollback()
                    except Exception:
                        if self._flow_store.in_transaction():
                            self._flow_store.rollback()
                        raise

                if include_debug_fields:
                    output["output_hash"] = self._output_hash(
                        {
                            "cursor": output.get("cursor"),
                            "status": output.get("status"),
                            "work": output.get("work", []),
                            "errors": output.get("errors", []),
                        }
                    )
                if output.get("status") == "paused":
                    pause_note_id = self._emit_pause_decision_request(
                        flow_id=flow.flow_id,
                        cursor=str(output.get("cursor") or ""),
                        goal=goal,
                        stage=cursor_stage,
                        requested_work=request_work,
                    )
                    if pause_note_id:
                        if include_debug_fields:
                            state_obj = output.get("state")
                            if isinstance(state_obj, dict):
                                frontier_obj = state_obj.get("frontier")
                                if not isinstance(frontier_obj, dict):
                                    frontier_obj = {}
                                    state_obj["frontier"] = frontier_obj
                                frontier_obj["pause_request_id"] = pause_note_id
                if idempotency_key:
                    self._store_idempotent(str(idempotency_key), request_hash, output)
                return output
            except Exception:
                if self._flow_store.in_transaction():
                    self._flow_store.rollback()
                raise

    def run_work(self, cursor: str, work_id: str) -> dict[str, Any]:
        decoded = self._decode_cursor(cursor)
        if decoded is None:
            raise ValueError("invalid cursor")
        flow_id, _ = decoded
        with self._lock:
            work = self._get_work(flow_id, work_id)
            if work is None:
                raise ValueError(f"unknown work_id: {work_id}")
            if work.status != "requested":
                raise ValueError(f"work_id is not pending: {work_id}")
            payload = json.loads(work.input_json)
            work_row = work

        execution = self._work_executor.execute(payload)
        return self._work_result_from_execution(
            work_id=work_id,
            payload=payload,
            execution=execution,
            attempt=int(work_row.attempt),
        )

    def count_requested_work(self, *, claimable_only: bool = False) -> int:
        with self._lock:
            return self._flow_store.count_requested_work(claimable_only=claimable_only)

    def _work_result_from_execution(
        self,
        *,
        work_id: str,
        payload: dict[str, Any],
        execution: Any,
        attempt: int,
    ) -> dict[str, Any]:
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
                "attempt": int(attempt),
            },
        }

    def _submit_work_result(self, flow_id: str, work_result: dict[str, Any]) -> dict[str, Any]:
        # Multiple workers may advance flow state in parallel. Retry state conflicts
        # with a fresh cursor so the claimed work result is eventually applied.
        last_output: Optional[dict[str, Any]] = None
        for _ in range(3):
            with self._lock:
                flow = self._get_flow(flow_id)
                if flow is None:
                    raise ValueError(f"unknown flow_id: {flow_id}")
                cursor = self._encode_cursor(flow.flow_id, flow.state_version)
            output = self.continue_flow(
                {
                    "request_id": f"work-{work_result.get('work_id')}-{uuid.uuid4().hex[:8]}",
                    "cursor": cursor,
                    "work_results": [work_result],
                }
            )
            last_output = output
            if str(output.get("status") or "") != "failed":
                return output
            errors = output.get("errors") or []
            first = errors[0] if isinstance(errors, list) and errors and isinstance(errors[0], dict) else {}
            if str(first.get("code") or "") != "state_conflict":
                return output
        return last_output or {"status": "failed", "errors": [{"code": "state_conflict"}]}

    def _release_claim_for_retry(
        self,
        *,
        work: WorkRow,
        worker_id: str,
        error: str,
    ) -> str:
        with self._lock:
            self._flow_store.release_work_for_retry(
                work_id=work.work_id,
                worker_id=worker_id,
                error=error,
            )
            refreshed = self._get_work(work.flow_id, work.work_id)
        return str(refreshed.status) if refreshed is not None else "unknown"

    def process_requested_work_batch(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        lease_seconds: int = 120,
    ) -> dict[str, Any]:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required")
        with self._lock:
            claimed = self._flow_store.claim_requested_work(
                worker_id=worker,
                limit=max(1, int(limit)),
                lease_seconds=max(1, int(lease_seconds)),
            )

        result: dict[str, Any] = {
            "claimed": len(claimed),
            "processed": 0,
            "failed": 0,
            "dead_lettered": 0,
            "errors": [],
        }
        for work in claimed:
            payload = json.loads(work.input_json)
            attempt = int(work.attempt)
            try:
                execution = self._work_executor.execute(payload)
                work_result = self._work_result_from_execution(
                    work_id=work.work_id,
                    payload=payload,
                    execution=execution,
                    attempt=attempt,
                )
                output = self._submit_work_result(work.flow_id, work_result)
                with self._lock:
                    refreshed = self._get_work(work.flow_id, work.work_id)
                if refreshed is not None and refreshed.status in {"completed", "failed"}:
                    result["processed"] += 1
                    continue
                errors = output.get("errors") or []
                first = errors[0] if isinstance(errors, list) and errors and isinstance(errors[0], dict) else {}
                code = str(first.get("code") or "work_apply_failed")
                message = str(first.get("message") or "Continuation work result was not applied")
                status = self._release_claim_for_retry(
                    work=work,
                    worker_id=worker,
                    error=f"{code}: {message}",
                )
                if status in {"dead_letter", "dead_lettered"}:
                    result["dead_lettered"] += 1
                else:
                    result["failed"] += 1
                result["errors"].append(f"{work.work_id}: {code}: {message}")
            except Exception as exc:
                status = self._release_claim_for_retry(
                    work=work,
                    worker_id=worker,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if status in {"dead_letter", "dead_lettered"}:
                    result["dead_lettered"] += 1
                else:
                    result["failed"] += 1
                result["errors"].append(f"{work.work_id}: {type(exc).__name__}: {exc}")
        return result

    def close(self) -> None:
        self._flow_store.close()
