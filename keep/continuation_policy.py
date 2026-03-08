"""Continuation decision support and query.auto policy logic.

This module isolates strategy/discriminator computation from the core
continuation tick runtime to reduce engine complexity while preserving the
continue(input) -> output contract.
"""

from __future__ import annotations

import math
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from .continuation_env import ContinuationRuntimeEnv

logger = logging.getLogger(__name__)

DECISION_SUPPORT_VERSION = "ds.v1"
DECISION_STRATEGIES = {"single_lane_refine", "top2_plus_bridge", "explore_more"}
DEFAULT_DECISION_POLICY = {
    "margin_high": 0.18,
    "entropy_low": 0.45,
    "margin_low": 0.08,
    "entropy_high": 0.72,
    "lineage_strong": 0.75,
    "pivot_topk": 6,
    "max_pivots": 2,
}


class ContinuationDecisionPolicy:
    """Decision/discriminator policy for continuation query refinement."""

    def __init__(
        self,
        *,
        env: ContinuationRuntimeEnv,
        parse_where_tags: Callable[[dict[str, Any]], dict[str, Any]],
        pipeline_limit: Callable[[dict[str, Any], int], int],
        normalize_metadata_level: Callable[[Any], str],
        as_int: Callable[[Any, int], int],
        is_decision_tag_key: Callable[[str], bool],
    ) -> None:
        self._env = env
        self._parse_where_tags = parse_where_tags
        self._pipeline_limit = pipeline_limit
        self._normalize_metadata_level = normalize_metadata_level
        self._as_int = as_int
        self._is_decision_tag_key = is_decision_tag_key

    @staticmethod
    def empty_discriminators() -> dict[str, Any]:
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
    def as_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
        return max(low, min(high, value))

    def decision_policy_from_program(self, program: dict[str, Any]) -> dict[str, Any]:
        policy: Any = program.get("decision_policy")
        if not isinstance(policy, dict):
            params = program.get("params") if isinstance(program.get("params"), dict) else {}
            policy = params.get("decision_policy") if isinstance(params, dict) else None
        if not isinstance(policy, dict):
            policy = {}

        merged = dict(DEFAULT_DECISION_POLICY)
        merged["margin_high"] = self.clamp(self.as_float(policy.get("margin_high"), merged["margin_high"]))
        merged["entropy_low"] = self.clamp(self.as_float(policy.get("entropy_low"), merged["entropy_low"]))
        merged["margin_low"] = self.clamp(self.as_float(policy.get("margin_low"), merged["margin_low"]))
        merged["entropy_high"] = self.clamp(self.as_float(policy.get("entropy_high"), merged["entropy_high"]))
        merged["lineage_strong"] = self.clamp(
            self.as_float(policy.get("lineage_strong"), merged["lineage_strong"]),
        )
        merged["pivot_topk"] = min(max(self._as_int(policy.get("pivot_topk"), merged["pivot_topk"]), 1), 20)
        merged["max_pivots"] = min(max(self._as_int(policy.get("max_pivots"), merged["max_pivots"]), 1), 5)
        return merged

    @staticmethod
    def empty_lineage_signal() -> dict[str, Any]:
        return {
            "coverage_topk": 0.0,
            "dominant_concentration_topk": 0.0,
            "dominant": "",
            "distinct_topk": 0,
        }

    def lineage_signal(
        self, evidence: list[dict[str, Any]], *, field: str, topk: int,
    ) -> dict[str, Any]:
        window = evidence[:topk]
        if not window:
            return self.empty_lineage_signal()

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
            return self.empty_lineage_signal()

        dominant, dominant_count = max(counts.items(), key=lambda item: item[1])
        coverage = self.clamp(present / max(1, len(window)))
        concentration = self.clamp(dominant_count / max(1, present))
        return {
            "coverage_topk": round(coverage, 4),
            "dominant_concentration_topk": round(concentration, 4),
            "dominant": dominant,
            "distinct_topk": int(len(counts)),
        }

    def lineage_signals(self, evidence: list[dict[str, Any]], *, topk: int) -> dict[str, Any]:
        return {
            "version": self.lineage_signal(evidence, field="version", topk=topk),
            "part": self.lineage_signal(evidence, field="part", topk=topk),
        }

    @staticmethod
    def decision_override_from_payload(
        payload: dict[str, Any],
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

    def candidate_subject_keys(
        self, frame_request: dict[str, Any], evidence: list[dict[str, Any]],
    ) -> list[str]:
        candidates: list[str] = []
        where_tags = self._parse_where_tags(frame_request)
        for key in where_tags:
            k = str(key).strip()
            if self._is_decision_tag_key(k) and k not in candidates:
                candidates.append(k)
        for row in evidence[:12]:
            metadata = row.get("metadata") if isinstance(row, dict) else None
            tags = metadata.get("tags") if isinstance(metadata, dict) else None
            if not isinstance(tags, dict):
                continue
            for key in sorted(tags.keys()):
                k = str(key).strip()
                if self._is_decision_tag_key(k) and k not in candidates:
                    candidates.append(k)
                if len(candidates) >= 12:
                    break
            if len(candidates) >= 12:
                break

        tag_kind_cache: dict[str, str] = {}
        facets: list[str] = []
        edges: list[str] = []
        for key in candidates:
            kind = self.tag_key_kind(key, cache=tag_kind_cache)
            if kind == "edge":
                edges.append(key)
            else:
                facets.append(key)
        return facets + edges

    def tag_key_kind(self, key: str, *, cache: Optional[dict[str, str]] = None) -> str:
        k = str(key or "").strip()
        if not k:
            return "facet"
        if cache is not None and k in cache:
            return cache[k]

        kind = "facet"
        try:
            doc_coll = self._env.resolve_doc_collection()
            tagdoc = self._env.get_document(f".tag/{k}", collection=doc_coll)
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
    def best_fact_from_counts(
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

    def tag_profile(self, evidence: list[dict[str, Any]], *, topk: int) -> dict[str, Any]:
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
                if not self._is_decision_tag_key(k):
                    continue
                keyset.add(k)

        tag_kind_cache: dict[str, str] = {}
        edge_keys: list[str] = []
        facet_keys: list[str] = []
        for key in sorted(keyset):
            if self.tag_key_kind(key, cache=tag_kind_cache) == "edge":
                edge_keys.append(key)
            else:
                facet_keys.append(key)
        return {
            "edge_key_count": len(edge_keys),
            "facet_key_count": len(facet_keys),
            "edge_keys": edge_keys,
            "facet_keys": facet_keys,
        }

    def query_stats(
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
            top_margin = self.clamp((ranked[0] - ranked[1]) / denom)
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
                lane_entropy = self.clamp(entropy / math.log(len(probs)))
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
        pivot_coverage = self.clamp(covered / max(1, len(window)))

        current_ids = [str(row.get("id")) for row in evidence if row.get("id")]
        prev_ids = {str(item) for item in previous_evidence_ids if item}
        gain = len([eid for eid in current_ids if eid not in prev_ids])
        expansion_yield = self.clamp(gain / max(1, len(current_ids)))
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
        temporal_alignment = 0.5 if dated_count == 0 else self.clamp(recent_count / dated_count)

        return {
            "lane_entropy": round(lane_entropy, 4),
            "top1_top2_margin": round(top_margin, 4),
            "pivot_coverage_topk": round(pivot_coverage, 4),
            "expansion_yield_prev_step": round(expansion_yield, 4),
            "cost_per_gain_prev_step": round(cost_per_gain, 4),
            "temporal_alignment": round(temporal_alignment, 4),
        }

    def choose_strategy(
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

        margin = self.as_float(query_stats.get("top1_top2_margin"), 0.0)
        entropy = self.as_float(query_stats.get("lane_entropy"), 1.0)
        margin_high = self.as_float(policy.get("margin_high"), DEFAULT_DECISION_POLICY["margin_high"])
        entropy_low = self.as_float(policy.get("entropy_low"), DEFAULT_DECISION_POLICY["entropy_low"])
        margin_low = self.as_float(policy.get("margin_low"), DEFAULT_DECISION_POLICY["margin_low"])
        entropy_high = self.as_float(policy.get("entropy_high"), DEFAULT_DECISION_POLICY["entropy_high"])
        lineage_strong = self.as_float(policy.get("lineage_strong"), DEFAULT_DECISION_POLICY["lineage_strong"])

        version = lineage.get("version") if isinstance(lineage, dict) else {}
        part = lineage.get("part") if isinstance(lineage, dict) else {}
        version_dom = self.as_float(
            version.get("dominant_concentration_topk") if isinstance(version, dict) else None, 0.0,
        )
        part_dom = self.as_float(
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
    def pivot_ids(evidence: list[dict[str, Any]], *, strategy: str, max_pivots: int) -> list[str]:
        ids = [str(row.get("id")) for row in evidence if row.get("id")]
        if strategy == "single_lane_refine":
            return ids[: min(1, max_pivots)]
        if strategy == "top2_plus_bridge":
            return ids[: min(2, max_pivots)]
        return []

    def decision_capsule(
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
            candidates = self.candidate_subject_keys(frame_request, evidence)
            planner_payload = self._env.get_planner_priors(
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

        policy = self.decision_policy_from_program(program)
        topk = self._as_int(policy.get("pivot_topk"), DEFAULT_DECISION_POLICY["pivot_topk"])
        query_stats = self.query_stats(
            frame_request=frame_request,
            evidence=evidence,
            previous_evidence_ids=previous_evidence_ids,
            topk=topk,
        )
        lineage = self.lineage_signals(evidence, topk=topk)
        tag_profile = self.tag_profile(evidence, topk=topk)
        strategy, reason_codes = self.choose_strategy(
            query_stats=query_stats,
            lineage=lineage,
            policy=policy,
            staleness=staleness,
            decision_override=decision_override,
        )
        pivot_ids = self.pivot_ids(
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

    def dominant_tag_fact(self, evidence: list[dict[str, Any]]) -> Optional[str]:
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
                if not self._is_decision_tag_key(k):
                    continue
                kind = self.tag_key_kind(k, cache=tag_kind_cache)
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

        fact = self.best_fact_from_counts(facet_counts, total=total)
        if fact:
            return fact
        return self.best_fact_from_counts(edge_counts, total=total)

    def query_auto_next_frame_request(
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
        limit = self._pipeline_limit(current_frame_request, 10)
        strategy = str(decision_snapshot.get("strategy_chosen") or "explore_more")

        if strategy == "single_lane_refine":
            fact = self.dominant_tag_fact(evidence)
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

        broaden_limit = min(max(limit + 5, 1), 50)
        return {
            "seed": {"mode": "query", "value": str(seed.get("value"))},
            "pipeline": [{"op": "slice", "args": {"limit": broaden_limit}}],
            "budget": dict(budget),
            "options": {"deep": True, "metadata": metadata_level},
        }

    def top_tag_facts(
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
                if not self._is_decision_tag_key(k):
                    continue
                kind = self.tag_key_kind(k, cache=tag_kind_cache)
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
    def query_auto_branch_utility(
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

    def query_auto_top2_plus_bridge_branches(
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
        limit = self._pipeline_limit(current_frame_request, 10)
        query_value = str(seed.get("value"))

        branches: list[dict[str, Any]] = []
        top_facts = self.top_tag_facts(evidence, max_facts=2)
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
