"""Query result statistics for state-doc predicate evaluation.

Enriches find action output with statistics that state-doc predicates
reference: margin, entropy, lineage_strong, dominant_lineage_tags,
and top_facet_tags.  The find action returns raw {results, count};
the runtime calls enrich_find_output() before binding the output to
a rule's id.

See STATE-ACTIONS.md §4 for the full statistics table.
"""

from __future__ import annotations

import math
from typing import Any


def enrich_find_output(output: dict[str, Any]) -> dict[str, Any]:
    """Attach query statistics to a find action's output dict.

    Input: ``{results: [{id, summary, tags, score}, ...], count: N}``
    Output: same dict plus ``margin``, ``entropy``, ``lineage_strong``,
    ``dominant_lineage_tags``, and ``top_facet_tags``.

    List-mode results (no scores) get ``None`` for score-based stats.
    """
    results = output.get("results")
    if not isinstance(results, list):
        return output

    enriched = dict(output)

    scores = [r["score"] for r in results
              if isinstance(r, dict) and isinstance(r.get("score"), (int, float))]
    has_scores = bool(scores)

    enriched["margin"] = _margin(scores) if has_scores else None
    enriched["entropy"] = _entropy(scores) if has_scores else None

    lineage = _lineage(results)
    enriched["lineage_strong"] = lineage["concentration"]
    enriched["dominant_lineage_tags"] = lineage["dominant_tags"]

    enriched["top_facet_tags"] = _top_facet_tags(results)

    return enriched


# ---------------------------------------------------------------------------
# Score-based statistics
# ---------------------------------------------------------------------------

def _margin(scores: list[float]) -> float:
    """Normalized gap between top two scores.  1.0 = dominant, 0.0 = tied."""
    if len(scores) < 2:
        return 1.0 if scores else 0.0
    ranked = sorted(scores, reverse=True)
    denom = max(abs(ranked[0]), 1e-9)
    return max(0.0, min(1.0, (ranked[0] - ranked[1]) / denom))


def _entropy(scores: list[float]) -> float:
    """Normalized Shannon entropy of score distribution.  0 = peaked, 1 = uniform."""
    if len(scores) <= 1:
        return 0.0
    weights = [max(s, 0.0) for s in scores]
    total = sum(weights)
    if total <= 0:
        return 1.0
    probs = [w / total for w in weights]
    raw = -sum(p * math.log(p) for p in probs if p > 0)
    return max(0.0, min(1.0, raw / math.log(len(probs))))


# ---------------------------------------------------------------------------
# Lineage statistics
# ---------------------------------------------------------------------------

_LINEAGE_KEYS = ("_base_id", "_version_of")


def _lineage(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute version/part lineage concentration from result tags.

    Looks at ``_base_id`` and ``_version_of`` tags to find items that
    share a common ancestor.  Returns the concentration of the dominant
    lineage group and that group's representative tags.
    """
    if not results:
        return {"concentration": 0.0, "dominant_tags": None}

    # Count lineage roots
    counts: dict[str, int] = {}
    tag_by_root: dict[str, dict[str, Any]] = {}

    for r in results:
        if not isinstance(r, dict):
            continue
        tags = r.get("tags")
        if not isinstance(tags, dict):
            continue
        root = None
        for key in _LINEAGE_KEYS:
            val = tags.get(key)
            if isinstance(val, str) and val.strip():
                root = val.strip()
                break
        if root is None:
            continue
        counts[root] = counts.get(root, 0) + 1
        if root not in tag_by_root:
            tag_by_root[root] = dict(tags)

    if not counts:
        return {"concentration": 0.0, "dominant_tags": None}

    dominant_root, dominant_count = max(counts.items(), key=lambda x: x[1])
    present = sum(counts.values())
    concentration = round(dominant_count / max(present, 1), 4)

    dominant_tags = tag_by_root.get(dominant_root)

    return {
        "concentration": concentration,
        "dominant_tags": dominant_tags,
    }


# ---------------------------------------------------------------------------
# Facet tag statistics
# ---------------------------------------------------------------------------

def _top_facet_tags(
    results: list[dict[str, Any]], *, max_tags: int = 3,
) -> list[dict[str, Any]]:
    """Extract the most common facet tag constraints from results.

    Returns a list of single-key tag dicts like ``[{"topic": "auth"}, ...]``
    ordered by frequency.  System tags (``_``-prefixed) are excluded.
    """
    if not results:
        return []

    pair_counts: dict[tuple[str, str], int] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        tags = r.get("tags")
        if not isinstance(tags, dict):
            continue
        for key, raw in tags.items():
            k = str(key).strip()
            if not k or k.startswith("_"):
                continue
            values = _tag_values(raw)
            for val in values[:4]:
                pair = (k, val)
                pair_counts[pair] = pair_counts.get(pair, 0) + 1

    if not pair_counts:
        return []

    # Filter to pairs appearing 2+ times, then rank
    frequent = [(pair, count) for pair, count in pair_counts.items() if count >= 2]
    if not frequent:
        return []

    frequent.sort(key=lambda item: (-item[1], item[0][0], item[0][1]))

    return [{key: value} for (key, value), _ in frequent[:max_tags]]


def _tag_values(raw: Any) -> list[str]:
    """Normalize a tag value to a list of string values."""
    if isinstance(raw, list):
        return [str(v).strip() for v in raw
                if isinstance(v, (str, int, float, bool)) and str(v).strip()]
    if isinstance(raw, (str, int, float, bool)):
        s = str(raw).strip()
        return [s] if s else []
    return []
