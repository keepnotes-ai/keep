from __future__ import annotations

"""Store profile statistics for agent query planning."""

import json
from collections import Counter
from typing import Any

from . import action


def _top_k(counter: Counter, k: int = 10) -> dict[str, int]:
    return {v: c for v, c in counter.most_common(k)}


@action(id="stats")
class Stats:
    """Compute store profile statistics.

    Returns tag distributions, date histograms, structural counts,
    and edge fan-in/fan-out — everything an agent needs to plan
    queries effectively.
    """

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        top_k = int(params.get("top_k") or 10)

        conn = context.get_db_connection()
        collection = context.get_collection()

        # --- Global counts ---
        total = conn.execute(
            "SELECT COUNT(*) FROM documents WHERE collection = ?",
            (collection,),
        ).fetchone()[0]

        if total == 0:
            return {"total": 0, "tags": {}, "dates": {}, "structure": {}}

        # --- Scan tags ---
        rows = conn.execute(
            "SELECT id, tags_json FROM documents WHERE collection = ?",
            (collection,),
        ).fetchall()

        # Discover edge tag keys from .tag/* specs
        edge_tag_keys = set()
        spec_rows = conn.execute(
            "SELECT id, tags_json FROM documents WHERE collection = ? AND id LIKE '.tag/%'",
            (collection,),
        ).fetchall()
        for spec_id, spec_tags_json in spec_rows:
            spec_tags = json.loads(spec_tags_json)
            if "_inverse" in spec_tags:
                parts = spec_id.split("/")
                if len(parts) >= 2:
                    edge_tag_keys.add(parts[1])

        # Accumulate per-key stats
        tag_key_counts: Counter = Counter()
        tag_value_counts: dict[str, Counter] = {}
        source_counts: Counter = Counter()
        # For edge fan-out: track values-per-item
        edge_fan_outs: dict[str, list[int]] = {}

        for doc_id, tags_json in rows:
            tags = json.loads(tags_json)
            for k, v in tags.items():
                if k.startswith("_"):
                    if k == "_source":
                        source_counts[v] += 1
                    continue
                tag_key_counts[k] += 1
                if k not in tag_value_counts:
                    tag_value_counts[k] = Counter()
                if isinstance(v, list):
                    for vi in v:
                        tag_value_counts[k][str(vi)] += 1
                    if k in edge_tag_keys:
                        edge_fan_outs.setdefault(k, []).append(len(v))
                else:
                    tag_value_counts[k][str(v)] += 1
                    if k in edge_tag_keys:
                        edge_fan_outs.setdefault(k, []).append(1)

        # Build per-key output
        tags_out: dict[str, Any] = {}
        for k, cnt in tag_key_counts.most_common():
            vc = tag_value_counts[k]
            distinct = len(vc)
            unique = sum(1 for c in vc.values() if c == 1)
            coverage = round(cnt / total, 3)
            is_categorical = distinct <= 20 and distinct < cnt * 0.5
            is_edge = k in edge_tag_keys

            entry: dict[str, Any] = {
                "count": cnt,
                "distinct": distinct,
                "unique": unique,
                "coverage": coverage,
                "categorical": is_categorical,
                "edge": is_edge,
                "top": _top_k(vc, top_k),
            }

            if is_edge and k in edge_fan_outs:
                fan_outs = edge_fan_outs[k]
                fan_ins = list(vc.values())
                entry["fan_out"] = {
                    "avg": round(sum(fan_outs) / len(fan_outs), 1),
                    "max": max(fan_outs),
                }
                entry["fan_in"] = {
                    "avg": round(sum(fan_ins) / len(fan_ins), 1) if fan_ins else 0,
                    "max": max(fan_ins) if fan_ins else 0,
                }

            tags_out[k] = entry

        # --- Date histograms ---
        def _date_histogram(column: str) -> dict[str, Any]:
            row = conn.execute(
                f"SELECT MIN({column}), MAX({column}) FROM documents WHERE collection = ?",
                (collection,),
            ).fetchone()
            min_date = row[0][:10] if row[0] else None
            max_date = row[1][:10] if row[1] else None

            annual = conn.execute(
                f"SELECT substr({column}, 1, 4) as y, COUNT(*) FROM documents "
                f"WHERE collection = ? GROUP BY y ORDER BY COUNT(*) DESC LIMIT ?",
                (collection, top_k),
            ).fetchall()

            monthly = conn.execute(
                f"SELECT substr({column}, 1, 7) as m, COUNT(*) FROM documents "
                f"WHERE collection = ? GROUP BY m ORDER BY COUNT(*) DESC LIMIT ?",
                (collection, top_k),
            ).fetchall()

            daily = conn.execute(
                f"SELECT substr({column}, 1, 10) as d, COUNT(*) FROM documents "
                f"WHERE collection = ? GROUP BY d ORDER BY COUNT(*) DESC LIMIT ?",
                (collection, top_k),
            ).fetchall()

            return {
                "min": min_date,
                "max": max_date,
                "annual": {r[0]: r[1] for r in annual},
                "monthly": {r[0]: r[1] for r in monthly},
                "daily": {r[0]: r[1] for r in daily},
            }

        dates_out = {
            "created": _date_histogram("created_at"),
            "updated": _date_histogram("updated_at"),
            "accessed": _date_histogram("accessed_at"),
        }

        # --- Structural stats ---
        version_rows = conn.execute(
            "SELECT id, COUNT(*) as cnt FROM document_versions "
            "WHERE collection = ? GROUP BY id",
            (collection,),
        ).fetchall()
        version_hist: Counter = Counter()
        for _, cnt in version_rows:
            version_hist[min(cnt, 100)] += 1

        parts_count = conn.execute(
            "SELECT COUNT(DISTINCT substr(id, 1, instr(id, '@p') - 1)) FROM documents "
            "WHERE collection = ? AND id LIKE '%@p%'",
            (collection,),
        ).fetchone()[0]

        structure_out = {
            "sources": dict(source_counts.most_common()),
            "with_versions": len(version_rows),
            "version_histogram": {str(k): version_hist[k] for k in sorted(version_hist)},
            "with_parts": parts_count,
        }

        return {
            "total": total,
            "tags": tags_out,
            "dates": dates_out,
            "structure": structure_out,
        }
