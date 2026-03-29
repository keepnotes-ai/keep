"""Minimal work queue backed by the existing continue_work SQLite table.

Provides enqueue/claim/complete/fail semantics without the FlowEngine
frame/decision/mutation machinery.  Reuses the same table schema and
DB file so there is no migration.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .const import SQLITE_BUSY_TIMEOUT_MS

# Fixed flow_id for directly-enqueued work (no FlowEngine flow).
_DIRECT_FLOW_ID = "_direct"


@dataclass
class WorkItem:
    """A claimed work item ready for execution."""

    work_id: str
    kind: str
    input: dict[str, Any]
    attempt: int
    priority: int = 5
    supersede_key: Optional[str] = None
    created_at: Optional[str] = None


class WorkQueue:
    """SQLite-backed work queue using the existing continue_work table.

    This is a focused subset of SQLiteFlowStore's work methods,
    without flow/event/mutation/idempotency tables.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.Lock()
        self._init_db()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False, isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")

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
        self._migrate()
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_continue_work_flow_status
            ON continue_work(flow_id, status)
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_continue_work_claimable
            ON continue_work(status, retry_after, lease_until, created_at)
        """)

    def _migrate(self) -> None:
        """Add columns introduced after the initial schema."""
        columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(continue_work)").fetchall()
        }

        def _add(col: str, coldef: str) -> None:
            try:
                self._conn.execute(f"ALTER TABLE continue_work ADD COLUMN {col} {coldef}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise

        if "claimed_by" not in columns:
            _add("claimed_by", "TEXT")
        if "claimed_at" not in columns:
            _add("claimed_at", "TEXT")
        if "lease_until" not in columns:
            _add("lease_until", "TEXT")
        if "retry_after" not in columns:
            _add("retry_after", "TEXT")
        if "last_error" not in columns:
            _add("last_error", "TEXT")
        if "max_attempts" not in columns:
            _add("max_attempts", "INTEGER NOT NULL DEFAULT 5")
        if "dead_lettered_at" not in columns:
            _add("dead_lettered_at", "TEXT")
        if "supersede_key" not in columns:
            _add("supersede_key", "TEXT")
            try:
                self._conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_continue_work_supersede
                    ON continue_work(supersede_key, status, created_at)
                """)
            except sqlite3.OperationalError:
                pass
        if "priority" not in columns:
            _add("priority", "INTEGER NOT NULL DEFAULT 5")

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    def enqueue(
        self,
        kind: str,
        input_data: dict[str, Any],
        *,
        supersede_key: Optional[str] = None,
        priority: int = 5,
    ) -> str:
        """Insert a new work item with status 'requested'.

        ``priority`` controls processing order (0 = first, 9 = last).
        Thread-safe: serializes access to the shared SQLite connection.
        Returns the work_id.
        """
        work_id = f"w_{uuid.uuid4().hex[:10]}"
        now = self._now()
        input_json = json.dumps(input_data, ensure_ascii=False, default=str)
        pri = max(0, min(9, int(priority)))
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO continue_work(
                    work_id, flow_id, kind, status, input_json, output_contract_json,
                    result_json, attempt, created_at, updated_at, supersede_key, priority
                ) VALUES (?, ?, ?, 'requested', ?, '{}', NULL, 1, ?, ?, ?, ?)
                """,
                (work_id, _DIRECT_FLOW_ID, kind, input_json, now, now, supersede_key, pri),
            )
            if supersede_key:
                self._supersede_prior(supersede_key, work_id)
        return work_id

    def _supersede_prior(self, supersede_key: str, keep_work_id: str) -> int:
        """Mark older unclaimed work with the same key as superseded.

        Caller must already hold self._lock.
        """
        now = self._now()
        cursor = self._conn.execute(
            """
            UPDATE continue_work
            SET status = 'superseded', updated_at = ?
            WHERE supersede_key = ?
              AND work_id != ?
              AND status = 'requested'
              AND claimed_by IS NULL
            """,
            (now, supersede_key, keep_work_id),
        )
        return cursor.rowcount

    # ------------------------------------------------------------------
    # Claim
    # ------------------------------------------------------------------

    def claim(
        self,
        worker_id: str,
        *,
        limit: int = 10,
        lease_seconds: int = 120,  # see const.DEFAULT_LEASE_SECONDS
    ) -> list[WorkItem]:
        """Claim up to *limit* requested items for *worker_id*."""
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required")

        lease_seconds = max(int(lease_seconds), 1)
        now = self._now()
        lease_until = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + lease_seconds,
            tz=timezone.utc,
        ).isoformat()

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """
                    SELECT work_id
                    FROM continue_work
                    WHERE status = 'requested'
                      AND dead_lettered_at IS NULL
                      AND (retry_after IS NULL OR retry_after <= ?)
                      AND (claimed_by IS NULL OR lease_until IS NULL OR lease_until <= ?)
                    ORDER BY priority ASC, created_at ASC
                    LIMIT ?
                    """,
                    (now, now, max(int(limit), 1)),
                ).fetchall()
                work_ids = [str(r["work_id"]) for r in rows]
                if not work_ids:
                    self._conn.commit()
                    return []

                self._conn.executemany(
                    """
                    UPDATE continue_work
                    SET claimed_by = ?, claimed_at = ?, lease_until = ?, updated_at = ?
                    WHERE work_id = ?
                    """,
                    [(worker, now, lease_until, now, wid) for wid in work_ids],
                )

                placeholders = ", ".join("?" for _ in work_ids)
                claimed = self._conn.execute(
                    f"""
                    SELECT work_id, kind, input_json, attempt, priority, supersede_key, created_at
                    FROM continue_work
                    WHERE work_id IN ({placeholders})
                    ORDER BY created_at ASC
                    """,
                    tuple(work_ids),
                ).fetchall()
                self._conn.commit()
                return [self._to_item(r) for r in claimed]
            except Exception:
                self._conn.rollback()
                raise

    @staticmethod
    def _to_item(row: sqlite3.Row) -> WorkItem:
        try:
            input_data = json.loads(row["input_json"])
        except (json.JSONDecodeError, TypeError):
            input_data = {}
        return WorkItem(
            work_id=str(row["work_id"]),
            kind=str(row["kind"]),
            input=input_data if isinstance(input_data, dict) else {},
            attempt=int(row["attempt"]),
            priority=int(row["priority"]) if row["priority"] is not None else 5,
            supersede_key=(
                str(row["supersede_key"]) if row["supersede_key"] is not None else None
            ),
            created_at=(
                str(row["created_at"]) if row["created_at"] is not None else None
            ),
        )

    # ------------------------------------------------------------------
    # Complete / Fail
    # ------------------------------------------------------------------

    def complete(self, work_id: str, result: dict[str, Any] | None = None) -> None:
        """Mark work as completed."""
        result_json = json.dumps(result or {}, ensure_ascii=False, default=str)
        with self._lock:
            self._conn.execute(
                """
                UPDATE continue_work
                SET status = 'completed', result_json = ?, updated_at = ?,
                    claimed_by = NULL, claimed_at = NULL, lease_until = NULL, retry_after = NULL
                WHERE work_id = ?
                """,
                (result_json, self._now(), work_id),
            )

    def fail(
        self,
        work_id: str,
        worker_id: str,
        error: Optional[str] = None,
        *,
        backoff_base: int = 30,   # see const.BACKOFF_BASE_SECONDS
        backoff_max: int = 3600,  # see const.BACKOFF_MAX_SECONDS
    ) -> None:
        """Release work for retry, or dead-letter if max attempts exceeded."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT attempt, max_attempts
                FROM continue_work
                WHERE work_id = ? AND status = 'requested'
                  AND dead_lettered_at IS NULL AND claimed_by = ?
                """,
                (work_id, worker_id),
            ).fetchone()
            if row is None:
                return

            attempt = int(row["attempt"]) if row["attempt"] is not None else 1
            max_attempts = int(row["max_attempts"]) if row["max_attempts"] is not None else 5
            now = self._now()

            if attempt >= max_attempts:
                self._dead_letter_locked(work_id, worker_id, error)
                return

            base = max(int(backoff_base), 1)
            cap = max(int(backoff_max), base)
            delay = min(base * (2 ** max(attempt - 1, 0)), cap)
            retry_after = datetime.fromtimestamp(
                datetime.now(timezone.utc).timestamp() + delay,
                tz=timezone.utc,
            ).isoformat()
            self._conn.execute(
                """
                UPDATE continue_work
                SET retry_after = ?, last_error = ?, attempt = attempt + 1,
                    claimed_by = NULL, claimed_at = NULL, lease_until = NULL,
                    updated_at = ?
                WHERE work_id = ? AND claimed_by = ?
                """,
                (retry_after, error, now, work_id, worker_id),
            )

    def _dead_letter_locked(
        self, work_id: str, worker_id: str, error: Optional[str] = None,
    ) -> None:
        """Caller must already hold self._lock."""
        now = self._now()
        self._conn.execute(
            """
            UPDATE continue_work
            SET status = 'dead_letter', dead_lettered_at = ?, last_error = ?,
                claimed_by = NULL, claimed_at = NULL, lease_until = NULL,
                retry_after = NULL, updated_at = ?
            WHERE work_id = ? AND status = 'requested'
              AND dead_lettered_at IS NULL AND claimed_by = ?
            """,
            (now, error, now, work_id, worker_id),
        )

    def release_stale_leases(self, current_worker: str) -> int:
        """Release leases held by workers other than *current_worker*.

        Called at daemon startup to reclaim work items that were leased
        by a previous daemon instance that exited without completing them.
        Returns the number of items released.
        """
        now = self._now()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE continue_work
                SET claimed_by = NULL, claimed_at = NULL, lease_until = NULL,
                    updated_at = ?
                WHERE status = 'requested'
                  AND claimed_by IS NOT NULL
                  AND claimed_by != ?
                """,
                (now, current_worker),
            )
            return cursor.rowcount

    def cancel_by_item_ids(self, item_ids: set[str]) -> int:
        """Cancel unclaimed work items targeting any of the given item IDs.

        Marks matching ``requested`` items as ``superseded``.
        Already-claimed items are skipped (they'll be cleaned up later).
        Returns the number of cancelled items.
        """
        if not item_ids:
            return 0
        now = self._now()
        placeholders = ",".join("?" * len(item_ids))
        with self._lock:
            cursor = self._conn.execute(
                f"""
                UPDATE continue_work
                SET status = 'superseded', updated_at = ?
                WHERE status = 'requested'
                  AND claimed_by IS NULL
                  AND json_extract(input_json, '$.item_id') IN ({placeholders})
                """,
                (now, *item_ids),
            )
            return cursor.rowcount

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def count(self, *, claimable_only: bool = False) -> int:
        """Count requested work items."""
        with self._lock:
            if claimable_only:
                now = self._now()
                row = self._conn.execute(
                    """
                    SELECT COUNT(1) AS c FROM continue_work
                    WHERE status = 'requested'
                      AND (retry_after IS NULL OR retry_after <= ?)
                      AND (lease_until IS NULL OR lease_until <= ?)
                    """,
                    (now, now),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(1) AS c FROM continue_work WHERE status = 'requested'"
                ).fetchone()
            return int(row["c"]) if row is not None else 0

    def count_by_kind(self) -> dict[str, int]:
        """Count requested work items grouped by kind (task type).

        Returns dict ordered by minimum priority (processing order).
        """
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT kind, COUNT(1) AS c, MIN(priority) AS p FROM continue_work
                WHERE status = 'requested'
                GROUP BY kind
                ORDER BY p ASC, kind ASC
                """
            ).fetchall()
            return {row["kind"]: int(row["c"]) for row in rows}

    def list_pending(self, limit: int = 50) -> list[dict]:
        """List pending work items with their type, target, and input."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT work_id, kind, supersede_key, created_at, retry_after, input_json
                FROM continue_work
                WHERE status = 'requested'
                ORDER BY priority ASC, created_at ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            raw = d.pop("input_json", None)
            if raw:
                try:
                    d["input"] = json.loads(raw)
                except Exception:
                    d["input"] = {}
            else:
                d["input"] = {}
            result.append(d)
        return result

    def has_superseding(
        self, work_id: str, supersede_key: str, created_at: str,
    ) -> bool:
        """Check if a newer requested item exists for the same supersede_key."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1 FROM continue_work
                WHERE supersede_key = ? AND work_id != ?
                  AND status = 'requested' AND created_at > ?
                LIMIT 1
                """,
                (supersede_key, work_id, created_at),
            ).fetchone()
            return row is not None

    def purge(self) -> int:
        """Delete all requested (unclaimed) work items. Returns count deleted."""
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM continue_work WHERE status = 'requested'"
            )
            self._conn.commit()
            return cur.rowcount

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
