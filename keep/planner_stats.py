"""Planner statistics store for continuation discriminators.

Maintains precomputed priors (fanout, selectivity, facet cardinality) in a
separate SQLite database (planner_stats.db).  Updated incrementally via an
outbox table in documents.db that is populated by triggers on document and
edge mutations.

The store is fully rebuildable — it can be deleted and reconstructed from
canonical data at any time.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DRAIN_MAX_ITEMS = 50
DRAIN_MAX_MS = 500
STALE_STATS_SECONDS = 3600  # 1 hour before fallback_mode


# ---------------------------------------------------------------------------
# Scope key
# ---------------------------------------------------------------------------

def build_scope_key(
    tenant: str = "local",
    project: str = "default",
    namespace: str | None = None,
    partition: str | None = None,
) -> str:
    """Build a stable, opaque scope key from logical dimensions.

    Args:
        tenant: Tenant identifier (``"local"`` for CLI use).
        project: Project identifier.
        namespace: Optional document namespace (future).
        partition: Optional facet partition descriptor (e.g.
            ``"topic=physics"``).  Appended after the base hash for
            ``collection+facet_partition`` scopes.

    Returns:
        Hex-encoded SHA-256 hash (first 16 bytes / 32 hex chars).
    """
    parts = [tenant, project, namespace or ""]
    base = hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]
    if partition:
        ext = hashlib.sha256(partition.encode()).hexdigest()[:16]
        return f"{base}:{ext}"
    return base


# ---------------------------------------------------------------------------
# PlannerStatsStore
# ---------------------------------------------------------------------------

class PlannerStatsStore:
    """Manages ``planner_stats.db`` — precomputed planner priors."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._lock = threading.RLock()
        self._init_db()

    # ----- DB lifecycle -----------------------------------------------------

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._create_tables()

    def _execute(self, sql: str, params: tuple | list = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, params)

    def _create_tables(self) -> None:
        self._execute("""
            CREATE TABLE IF NOT EXISTS planner_stat (
                stat_id       TEXT PRIMARY KEY,
                metric_family TEXT NOT NULL,
                scope_key     TEXT NOT NULL,
                subject_key   TEXT NOT NULL,
                value_json    TEXT NOT NULL,
                sample_n      INTEGER NOT NULL,
                updated_at    TEXT NOT NULL
            )
        """)
        self._execute("""
            CREATE INDEX IF NOT EXISTS idx_planner_stat_lookup
            ON planner_stat (metric_family, scope_key, subject_key)
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS planner_node_stat (
                node_id       TEXT NOT NULL,
                scope_key     TEXT NOT NULL,
                metric_family TEXT NOT NULL,
                value         REAL NOT NULL,
                updated_at    TEXT NOT NULL,
                PRIMARY KEY (node_id, scope_key, metric_family)
            )
        """)

        self._execute("""
            CREATE TABLE IF NOT EXISTS planner_watermark (
                stream     TEXT PRIMARY KEY,
                offset_val TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None  # type: ignore[assignment]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ----- Stat ID ----------------------------------------------------------

    @staticmethod
    def _stat_id(metric_family: str, scope_key: str, subject_key: str) -> str:
        raw = f"{metric_family}|{scope_key}|{subject_key}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    # ----- Upsert / delete --------------------------------------------------

    def upsert_stat(
        self,
        metric_family: str,
        scope_key: str,
        subject_key: str,
        value_json: dict | str,
        sample_n: int,
    ) -> None:
        """Idempotent upsert of a planner stat row."""
        sid = self._stat_id(metric_family, scope_key, subject_key)
        val = json.dumps(value_json) if isinstance(value_json, dict) else value_json
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO planner_stat
                    (stat_id, metric_family, scope_key, subject_key,
                     value_json, sample_n, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (sid, metric_family, scope_key, subject_key, val, sample_n, now),
            )
            self._conn.commit()

    def delete_stat(
        self, metric_family: str, scope_key: str, subject_key: str,
    ) -> None:
        sid = self._stat_id(metric_family, scope_key, subject_key)
        with self._lock:
            self._conn.execute(
                "DELETE FROM planner_stat WHERE stat_id = ?", (sid,)
            )
            self._conn.commit()

    # ----- Read API ---------------------------------------------------------

    def get_priors(
        self,
        scope_key: str,
        metric_families: list[str] | None = None,
        subject_keys: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Return precomputed priors grouped by metric family.

        Returns::

            {
                "expansion.fanout": {
                    "predicate_x": {"mean": 3.7, "p50": 2.0, ...},
                    ...
                },
                "expansion.selectivity": { ... },
                "facet.cardinality": { ... },
            }
        """
        sql = "SELECT metric_family, subject_key, value_json FROM planner_stat WHERE scope_key = ?"
        params: list[Any] = [scope_key]

        if metric_families:
            ph = ",".join("?" for _ in metric_families)
            sql += f" AND metric_family IN ({ph})"
            params.extend(metric_families)

        if subject_keys:
            ph = ",".join("?" for _ in subject_keys)
            sql += f" AND subject_key IN ({ph})"
            params.extend(subject_keys)

        rows = self._execute(sql, params).fetchall()

        result: dict[str, dict[str, Any]] = {}
        for mf, sk, vj in rows:
            result.setdefault(mf, {})[sk] = json.loads(vj)
        return result

    def get_staleness(self, scope_key: str) -> dict[str, Any]:
        """Return staleness info for a scope."""
        row = self._execute(
            """
            SELECT MIN(updated_at) FROM planner_stat
            WHERE scope_key = ?
            """,
            (scope_key,),
        ).fetchone()

        if not row or row[0] is None:
            return {"stats_age_s": None, "fallback_mode": True}

        from datetime import datetime, timezone
        oldest = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age_s = (now - oldest).total_seconds()
        return {
            "stats_age_s": round(age_s, 1),
            "fallback_mode": age_s > STALE_STATS_SECONDS,
        }

    # ----- Metric computation -----------------------------------------------

    def compute_fanout(self, doc_store: Any, scope_key: str, collection: str) -> int:
        """Compute expansion.fanout for all predicates.  Returns count of stats upserted."""
        rows = doc_store._execute(
            """
            SELECT predicate, source_id, COUNT(target_id) as cnt
            FROM edges
            WHERE collection = ?
            GROUP BY predicate, source_id
            """,
            (collection,),
        ).fetchall()

        # Group by predicate
        by_pred: dict[str, list[int]] = {}
        for pred, _src, cnt in rows:
            by_pred.setdefault(pred, []).append(cnt)

        count = 0
        for pred, counts in by_pred.items():
            counts.sort()
            n = len(counts)
            value = {
                "mean": round(sum(counts) / n, 2) if n else 0,
                "p50": counts[n // 2] if n else 0,
                "p90": counts[int(n * 0.9)] if n else 0,
                "max": counts[-1] if n else 0,
            }
            self.upsert_stat("expansion.fanout", scope_key, pred, value, n)
            count += 1
        return count

    def compute_selectivity(self, doc_store: Any, scope_key: str, collection: str) -> int:
        """Compute expansion.selectivity for all predicates.  Returns count of stats upserted."""
        # Total source nodes in scope (all documents)
        total_row = doc_store._execute(
            "SELECT COUNT(*) FROM documents WHERE collection = ?",
            (collection,),
        ).fetchone()
        total = total_row[0] if total_row else 0

        if total == 0:
            return 0

        rows = doc_store._execute(
            """
            SELECT predicate, COUNT(DISTINCT source_id) as src_count
            FROM edges
            WHERE collection = ?
            GROUP BY predicate
            """,
            (collection,),
        ).fetchall()

        count = 0
        for pred, src_count in rows:
            value = {
                "selectivity": round(src_count / total, 4),
                "sources_total": total,
                "sources_with_hits": src_count,
            }
            self.upsert_stat("expansion.selectivity", scope_key, pred, value, total)
            count += 1
        return count

    def compute_facet_cardinality(self, doc_store: Any, scope_key: str, collection: str) -> int:
        """Compute facet.cardinality for all non-system tag keys.  Returns count of stats upserted."""
        # Get (key, value, count) triples
        pair_counts = doc_store.tag_pair_counts(collection)
        if not pair_counts:
            return 0

        # Group by key
        by_key: dict[str, list[tuple[str, int]]] = {}
        for (key, val), cnt in pair_counts.items():
            by_key.setdefault(key, []).append((val, cnt))

        count = 0
        for key, val_counts in by_key.items():
            distinct = len(val_counts)
            total = sum(c for _, c in val_counts)
            # Top values (descending by count)
            sorted_vc = sorted(val_counts, key=lambda x: x[1], reverse=True)
            top_values = [[v, c] for v, c in sorted_vc[:10]]
            # Shannon entropy
            entropy = 0.0
            if total > 0:
                for _, c in val_counts:
                    p = c / total
                    if p > 0:
                        entropy -= p * math.log2(p)

            value = {
                "distinct_values": distinct,
                "top_values": top_values,
                "entropy": round(entropy, 4),
            }
            self.upsert_stat("facet.cardinality", scope_key, key, value, total)
            count += 1
        return count

    def needs_rebuild(self, collection: str) -> bool:
        """True if no full rebuild has been recorded for this collection."""
        row = self._execute(
            "SELECT 1 FROM planner_watermark WHERE stream = ? LIMIT 1",
            (f"rebuild:{collection}",),
        ).fetchone()
        return row is None

    # ----- Drain / rebuild --------------------------------------------------

    def drain_outbox(
        self,
        doc_store: Any,
        collection: str,
        scope_key: str | None = None,
        max_items: int = DRAIN_MAX_ITEMS,
        max_ms: int = DRAIN_MAX_MS,
    ) -> dict[str, int]:
        """Drain planner outbox and update affected stats.

        Returns dict with ``processed``, ``failed``, ``skipped`` counts.
        """
        if scope_key is None:
            scope_key = build_scope_key()

        # Bootstrap: if no rebuild has ever run, do a full rebuild first.
        # This handles existing stores that migrated to schema v12.
        if self.needs_rebuild(collection):
            logger.info("Planner stats: initial rebuild for collection %s", collection)
            self.rebuild(doc_store, collection, scope_key=scope_key)
            return {"processed": 0, "failed": 0, "skipped": 0, "rebuilt": True}

        items = doc_store.dequeue_outbox(limit=max_items)
        if not items:
            return {"processed": 0, "failed": 0, "skipped": 0}

        deadline = time.monotonic() + max_ms / 1000.0

        # Collect affected predicates and tag keys for targeted recompute
        affected_predicates: set[str] = set()
        affected_tag_keys: set[str] = set()
        completed: list[int] = []
        failed: list[int] = []

        for item in items:
            if time.monotonic() > deadline:
                # Release remaining items
                remaining_ids = [
                    it["outbox_id"] for it in items
                    if it["outbox_id"] not in completed
                    and it["outbox_id"] not in failed
                ]
                if remaining_ids:
                    doc_store.fail_outbox(remaining_ids)
                break

            try:
                payload = json.loads(item["payload_json"])
                mutation = item["mutation"]

                if mutation.startswith("edge_"):
                    pred = payload.get("predicate")
                    if pred:
                        affected_predicates.add(pred)
                elif mutation.startswith("doc_"):
                    # Extract changed tag keys
                    if mutation == "doc_update":
                        old_tags = json.loads(payload.get("old_tags_json", "{}"))
                        new_tags = json.loads(payload.get("new_tags_json", "{}"))
                        changed = set(old_tags.keys()) | set(new_tags.keys())
                    else:
                        tags = json.loads(payload.get("tags_json", "{}"))
                        changed = set(tags.keys())
                    # Exclude system tags
                    affected_tag_keys.update(
                        k for k in changed if not k.startswith("_")
                    )

                completed.append(item["outbox_id"])
            except Exception:
                logger.warning(
                    "Failed to parse outbox item %d", item["outbox_id"],
                    exc_info=True,
                )
                failed.append(item["outbox_id"])

        # Recompute affected metrics
        try:
            if affected_predicates:
                self.compute_fanout(doc_store, scope_key, collection)
                self.compute_selectivity(doc_store, scope_key, collection)
            if affected_tag_keys:
                self.compute_facet_cardinality(doc_store, scope_key, collection)
        except Exception:
            logger.warning("Failed to recompute planner stats", exc_info=True)
            # Fail all items so they retry
            doc_store.fail_outbox(completed)
            return {"processed": 0, "failed": len(completed) + len(failed), "skipped": 0}

        doc_store.complete_outbox(completed)
        if failed:
            doc_store.fail_outbox(failed)

        return {
            "processed": len(completed),
            "failed": len(failed),
            "skipped": 0,
        }

    def rebuild(self, doc_store: Any, collection: str, scope_key: str | None = None) -> dict[str, int]:
        """Full rebuild of all metrics from canonical data.

        Returns dict with count of stats upserted per metric family.
        """
        if scope_key is None:
            scope_key = build_scope_key()

        result = {
            "expansion.fanout": self.compute_fanout(doc_store, scope_key, collection),
            "expansion.selectivity": self.compute_selectivity(doc_store, scope_key, collection),
            "facet.cardinality": self.compute_facet_cardinality(doc_store, scope_key, collection),
        }

        # Update watermark
        now = _utc_now()
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO planner_watermark
                    (stream, offset_val, updated_at)
                VALUES (?, ?, ?)
                """,
                (f"rebuild:{collection}", now, now),
            )
            self._conn.commit()

        # Clear outbox (all items are now stale after full rebuild)
        try:
            doc_store._execute(
                "DELETE FROM planner_outbox WHERE collection = ?",
                (collection,),
            )
            doc_store._conn.commit()
        except Exception:
            logger.debug("Could not clear outbox after rebuild", exc_info=True)

        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    """UTC timestamp without importing keep.types (avoids circular deps)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
