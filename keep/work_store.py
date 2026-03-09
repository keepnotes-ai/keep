"""Work store adapters.

Defines the persistence boundary for flow runtime state so the runtime
logic can be shared across local and hosted implementations.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Protocol


@dataclass
class FlowRow:
    """A flow record."""

    flow_id: str
    state_version: int
    status: str
    state_json: str


@dataclass
class WorkRow:
    """A work item record within a flow."""

    work_id: str
    flow_id: str
    kind: str
    status: str
    input_json: str
    output_contract_json: str
    attempt: int
    claimed_by: Optional[str] = None
    claimed_at: Optional[str] = None
    lease_until: Optional[str] = None
    retry_after: Optional[str] = None
    last_error: Optional[str] = None
    max_attempts: int = 5
    dead_lettered_at: Optional[str] = None


@dataclass
class MutationRow:
    """A mutation record tracking store operations from a flow."""

    mutation_id: str
    flow_id: str
    work_id: Optional[str]
    status: str
    op_json: str
    attempts: int
    error: Optional[str]


class FlowStore(Protocol):
    """Persistence boundary for flow state."""

    def begin_immediate(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def in_transaction(self) -> bool: ...

    def get_flow(self, flow_id: str) -> Optional[FlowRow]: ...
    def create_flow(self, state_json: str) -> FlowRow: ...
    def update_flow(
        self, flow_id: str, *, state_version: int, status: str, state_json: str,
    ) -> None: ...

    def load_idempotent(self, key: str) -> Optional[tuple[str, str]]: ...
    def store_idempotent(self, key: str, request_hash: str, output_json: str) -> None: ...

    def list_requested_work(self, flow_id: str) -> list[WorkRow]: ...
    def count_requested_work(self, *, claimable_only: bool = False) -> int: ...
    def get_work(self, flow_id: str, work_id: str) -> Optional[WorkRow]: ...
    def has_any_work_key(self, flow_id: str, key: str) -> bool: ...
    def has_completed_work_key(self, flow_id: str, key: str) -> bool: ...
    def insert_work(
        self, *, flow_id: str, kind: str, input_json: str, output_contract_json: str,
    ) -> str: ...
    def update_work_result(self, *, work_id: str, status: str, result_json: str) -> None: ...
    def claim_requested_work(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        lease_seconds: int = 120,
        flow_id: Optional[str] = None,
    ) -> list[WorkRow]: ...
    def renew_work_lease(
        self,
        *,
        work_id: str,
        worker_id: str,
        lease_seconds: int = 120,
    ) -> bool: ...
    def release_work_for_retry(
        self,
        *,
        work_id: str,
        worker_id: str,
        error: Optional[str] = None,
        backoff_base_seconds: int = 30,
        backoff_max_seconds: int = 3600,
    ) -> bool: ...
    def dead_letter_work(
        self,
        *,
        work_id: str,
        worker_id: str,
        error: Optional[str] = None,
    ) -> bool: ...

    def insert_event(self, *, flow_id: str, event_type: str, payload_json: str) -> None: ...
    def prune_events(self, flow_id: str, *, keep_last: int) -> None: ...

    def insert_pending_mutation(self, *, flow_id: str, work_id: Optional[str], op_json: str) -> str: ...
    def get_mutation(self, mutation_id: str) -> Optional[MutationRow]: ...
    def set_mutation_status(self, mutation_id: str, *, status: str, error: Optional[str] = None) -> None: ...
    def list_pending_mutations(self, *, flow_id: Optional[str] = None, limit: int = 100) -> list[MutationRow]: ...

    def close(self) -> None: ...


class SQLiteFlowStore:
    """SQLite-backed implementation of FlowStore."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
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
        self._migrate_continue_work()
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_continue_work_flow_status
            ON continue_work(flow_id, status)
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_continue_work_claimable
            ON continue_work(status, retry_after, lease_until, created_at)
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
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS continue_mutations (
                mutation_id TEXT PRIMARY KEY,
                flow_id TEXT NOT NULL,
                work_id TEXT,
                status TEXT NOT NULL,
                op_json TEXT NOT NULL,
                error TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_continue_mutations_status_created
            ON continue_mutations(status, created_at)
        """)

    def _migrate_continue_work(self) -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute("PRAGMA table_info(continue_work)").fetchall()
        }

        def _add_column(column: str, coldef: str) -> None:
            try:
                self._conn.execute(f"ALTER TABLE continue_work ADD COLUMN {column} {coldef}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise

        if "claimed_by" not in columns:
            _add_column("claimed_by", "TEXT")
        if "claimed_at" not in columns:
            _add_column("claimed_at", "TEXT")
        if "lease_until" not in columns:
            _add_column("lease_until", "TEXT")
        if "retry_after" not in columns:
            _add_column("retry_after", "TEXT")
        if "last_error" not in columns:
            _add_column("last_error", "TEXT")
        if "max_attempts" not in columns:
            _add_column("max_attempts", "INTEGER NOT NULL DEFAULT 5")
        if "dead_lettered_at" not in columns:
            _add_column("dead_lettered_at", "TEXT")

    @staticmethod
    def _work_row(row: sqlite3.Row) -> WorkRow:
        return WorkRow(
            work_id=str(row["work_id"]),
            flow_id=str(row["flow_id"]),
            kind=str(row["kind"]),
            status=str(row["status"]),
            input_json=str(row["input_json"]),
            output_contract_json=str(row["output_contract_json"]),
            attempt=int(row["attempt"]),
            claimed_by=str(row["claimed_by"]) if row["claimed_by"] is not None else None,
            claimed_at=str(row["claimed_at"]) if row["claimed_at"] is not None else None,
            lease_until=str(row["lease_until"]) if row["lease_until"] is not None else None,
            retry_after=str(row["retry_after"]) if row["retry_after"] is not None else None,
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            max_attempts=int(row["max_attempts"]) if row["max_attempts"] is not None else 5,
            dead_lettered_at=(
                str(row["dead_lettered_at"])
                if row["dead_lettered_at"] is not None
                else None
            ),
        )

    def begin_immediate(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def in_transaction(self) -> bool:
        return bool(self._conn.in_transaction)

    def get_flow(self, flow_id: str) -> Optional[FlowRow]:
        row = self._conn.execute(
            "SELECT flow_id, state_version, status, state_json FROM continue_flows WHERE flow_id = ?",
            (flow_id,),
        ).fetchone()
        if row is None:
            return None
        return FlowRow(
            flow_id=str(row["flow_id"]),
            state_version=int(row["state_version"]),
            status=str(row["status"]),
            state_json=str(row["state_json"]),
        )

    def create_flow(self, state_json: str) -> FlowRow:
        flow_id = f"f_{uuid.uuid4().hex[:10]}"
        now = self._now()
        self._conn.execute(
            """
            INSERT INTO continue_flows(flow_id, state_version, status, state_json, created_at, updated_at)
            VALUES (?, 0, 'running', ?, ?, ?)
            """,
            (flow_id, state_json, now, now),
        )
        return FlowRow(flow_id=flow_id, state_version=0, status="running", state_json=state_json)

    def update_flow(
        self, flow_id: str, *, state_version: int, status: str, state_json: str,
    ) -> None:
        self._conn.execute(
            """
            UPDATE continue_flows
            SET state_version = ?, status = ?, state_json = ?, updated_at = ?
            WHERE flow_id = ?
            """,
            (state_version, status, state_json, self._now(), flow_id),
        )

    def load_idempotent(self, key: str) -> Optional[tuple[str, str]]:
        row = self._conn.execute(
            "SELECT request_hash, output_json FROM continue_idempotency WHERE idempotency_key = ?",
            (key,),
        ).fetchone()
        if row is None:
            return None
        return str(row["request_hash"]), str(row["output_json"])

    def store_idempotent(self, key: str, request_hash: str, output_json: str) -> None:
        self._conn.execute(
            """
            INSERT OR REPLACE INTO continue_idempotency(idempotency_key, request_hash, output_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (key, request_hash, output_json, self._now()),
        )

    def list_requested_work(self, flow_id: str) -> list[WorkRow]:
        rows = self._conn.execute(
            """
            SELECT
                work_id, flow_id, kind, status, input_json, output_contract_json, attempt,
                claimed_by, claimed_at, lease_until, retry_after, last_error, max_attempts, dead_lettered_at
            FROM continue_work
            WHERE flow_id = ? AND status = 'requested'
            ORDER BY created_at ASC
            """,
            (flow_id,),
        ).fetchall()
        return [self._work_row(row) for row in rows]

    def count_requested_work(self, *, claimable_only: bool = False) -> int:
        if claimable_only:
            now = self._now()
            row = self._conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM continue_work
                WHERE status = 'requested'
                  AND (retry_after IS NULL OR retry_after <= ?)
                  AND (lease_until IS NULL OR lease_until <= ?)
                """,
                (now, now),
            ).fetchone()
            return int(row["c"]) if row is not None else 0
        row = self._conn.execute(
            """
            SELECT COUNT(1) AS c
            FROM continue_work
            WHERE status = 'requested'
            """
        ).fetchone()
        return int(row["c"]) if row is not None else 0

    def get_work(self, flow_id: str, work_id: str) -> Optional[WorkRow]:
        row = self._conn.execute(
            """
            SELECT
                work_id, flow_id, kind, status, input_json, output_contract_json, attempt,
                claimed_by, claimed_at, lease_until, retry_after, last_error, max_attempts, dead_lettered_at
            FROM continue_work
            WHERE flow_id = ? AND work_id = ?
            """,
            (flow_id, work_id),
        ).fetchone()
        if row is None:
            return None
        return self._work_row(row)

    def has_any_work_key(self, flow_id: str, key: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM continue_work
            WHERE flow_id = ? AND kind = ?
            LIMIT 1
            """,
            (flow_id, key),
        ).fetchone()
        return row is not None

    def has_completed_work_key(self, flow_id: str, key: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM continue_work
            WHERE flow_id = ? AND kind = ? AND status = 'completed'
            LIMIT 1
            """,
            (flow_id, key),
        ).fetchone()
        return row is not None

    def insert_work(
        self, *, flow_id: str, kind: str, input_json: str, output_contract_json: str,
    ) -> str:
        work_id = f"w_{uuid.uuid4().hex[:10]}"
        now = self._now()
        self._conn.execute(
            """
            INSERT INTO continue_work(
                work_id, flow_id, kind, status, input_json, output_contract_json,
                result_json, attempt, created_at, updated_at
            ) VALUES (?, ?, ?, 'requested', ?, ?, NULL, 1, ?, ?)
            """,
            (work_id, flow_id, kind, input_json, output_contract_json, now, now),
        )
        return work_id

    def update_work_result(self, *, work_id: str, status: str, result_json: str) -> None:
        self._conn.execute(
            """
            UPDATE continue_work
            SET status = ?, result_json = ?, updated_at = ?,
                claimed_by = NULL, claimed_at = NULL, lease_until = NULL, retry_after = NULL
            WHERE work_id = ?
            """,
            (status, result_json, self._now(), work_id),
        )

    def claim_requested_work(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        lease_seconds: int = 120,
        flow_id: Optional[str] = None,
    ) -> list[WorkRow]:
        worker = str(worker_id or "").strip()
        if not worker:
            raise ValueError("worker_id is required")

        lease_seconds = max(int(lease_seconds), 1)
        now = self._now()
        lease_until = (
            datetime.now(timezone.utc).timestamp() + lease_seconds
        )
        lease_until_iso = datetime.fromtimestamp(lease_until, tz=timezone.utc).isoformat()

        self.begin_immediate()
        try:
            params: list[object] = [now, now]
            flow_filter = ""
            if flow_id:
                flow_filter = "AND flow_id = ?"
                params.append(str(flow_id))
            params.append(max(int(limit), 1))
            rows = self._conn.execute(
                f"""
                SELECT work_id
                FROM continue_work
                WHERE status = 'requested'
                  AND dead_lettered_at IS NULL
                  AND (retry_after IS NULL OR retry_after <= ?)
                  AND (
                        claimed_by IS NULL
                        OR lease_until IS NULL
                        OR lease_until <= ?
                  )
                  {flow_filter}
                ORDER BY created_at ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            work_ids = [str(row["work_id"]) for row in rows]
            if not work_ids:
                self.commit()
                return []

            self._conn.executemany(
                """
                UPDATE continue_work
                SET claimed_by = ?, claimed_at = ?, lease_until = ?, updated_at = ?
                WHERE work_id = ?
                """,
                [(worker, now, lease_until_iso, now, wid) for wid in work_ids],
            )

            placeholders = ", ".join("?" for _ in work_ids)
            claimed_rows = self._conn.execute(
                f"""
                SELECT
                    work_id, flow_id, kind, status, input_json, output_contract_json, attempt,
                    claimed_by, claimed_at, lease_until, retry_after, last_error, max_attempts, dead_lettered_at
                FROM continue_work
                WHERE work_id IN ({placeholders})
                ORDER BY created_at ASC
                """,
                tuple(work_ids),
            ).fetchall()
            self.commit()
            return [self._work_row(row) for row in claimed_rows]
        except Exception:
            self.rollback()
            raise

    def renew_work_lease(
        self,
        *,
        work_id: str,
        worker_id: str,
        lease_seconds: int = 120,
    ) -> bool:
        lease_seconds = max(int(lease_seconds), 1)
        now = self._now()
        lease_until = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + lease_seconds,
            tz=timezone.utc,
        ).isoformat()
        cursor = self._conn.execute(
            """
            UPDATE continue_work
            SET lease_until = ?, updated_at = ?
            WHERE work_id = ?
              AND status = 'requested'
              AND dead_lettered_at IS NULL
              AND claimed_by = ?
            """,
            (lease_until, now, work_id, worker_id),
        )
        return cursor.rowcount > 0

    def release_work_for_retry(
        self,
        *,
        work_id: str,
        worker_id: str,
        error: Optional[str] = None,
        backoff_base_seconds: int = 30,
        backoff_max_seconds: int = 3600,
    ) -> bool:
        row = self._conn.execute(
            """
            SELECT attempt, max_attempts
            FROM continue_work
            WHERE work_id = ?
              AND status = 'requested'
              AND dead_lettered_at IS NULL
              AND claimed_by = ?
            """,
            (work_id, worker_id),
        ).fetchone()
        if row is None:
            return False

        attempt = int(row["attempt"]) if row["attempt"] is not None else 1
        max_attempts = int(row["max_attempts"]) if row["max_attempts"] is not None else 5
        now = self._now()

        if attempt >= max_attempts:
            self._conn.execute(
                """
                UPDATE continue_work
                SET status = 'dead_letter',
                    dead_lettered_at = ?,
                    last_error = ?,
                    claimed_by = NULL,
                    claimed_at = NULL,
                    lease_until = NULL,
                    retry_after = NULL,
                    updated_at = ?
                WHERE work_id = ? AND claimed_by = ?
                """,
                (now, error, now, work_id, worker_id),
            )
            return True

        base = max(int(backoff_base_seconds), 1)
        max_delay = max(int(backoff_max_seconds), base)
        delay = min(base * (2 ** max(attempt - 1, 0)), max_delay)
        retry_after = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() + delay,
            tz=timezone.utc,
        ).isoformat()
        self._conn.execute(
            """
            UPDATE continue_work
            SET retry_after = ?,
                last_error = ?,
                attempt = attempt + 1,
                claimed_by = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                updated_at = ?
            WHERE work_id = ? AND claimed_by = ?
            """,
            (retry_after, error, now, work_id, worker_id),
        )
        return True

    def dead_letter_work(
        self,
        *,
        work_id: str,
        worker_id: str,
        error: Optional[str] = None,
    ) -> bool:
        now = self._now()
        cursor = self._conn.execute(
            """
            UPDATE continue_work
            SET status = 'dead_letter',
                dead_lettered_at = ?,
                last_error = ?,
                claimed_by = NULL,
                claimed_at = NULL,
                lease_until = NULL,
                retry_after = NULL,
                updated_at = ?
            WHERE work_id = ?
              AND status = 'requested'
              AND dead_lettered_at IS NULL
              AND claimed_by = ?
            """,
            (now, error, now, work_id, worker_id),
        )
        return cursor.rowcount > 0

    def insert_event(self, *, flow_id: str, event_type: str, payload_json: str) -> None:
        self._conn.execute(
            """
            INSERT INTO continue_events(event_id, flow_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (f"e_{uuid.uuid4().hex[:12]}", flow_id, event_type, payload_json, self._now()),
        )

    def prune_events(self, flow_id: str, *, keep_last: int) -> None:
        self._conn.execute(
            """
            DELETE FROM continue_events
            WHERE flow_id = ?
              AND event_id NOT IN (
                SELECT event_id FROM continue_events
                WHERE flow_id = ?
                ORDER BY created_at DESC
                LIMIT ?
              )
            """,
            (flow_id, flow_id, max(int(keep_last), 1)),
        )

    def insert_pending_mutation(self, *, flow_id: str, work_id: Optional[str], op_json: str) -> str:
        material = f"{flow_id}|{work_id or ''}|{op_json}"
        mutation_id = f"m_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:20]}"
        now = self._now()
        self._conn.execute(
            """
            INSERT OR IGNORE INTO continue_mutations(
                mutation_id, flow_id, work_id, status, op_json, error, attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'pending', ?, NULL, 0, ?, ?)
            """,
            (mutation_id, flow_id, work_id, op_json, now, now),
        )
        return mutation_id

    def get_mutation(self, mutation_id: str) -> Optional[MutationRow]:
        row = self._conn.execute(
            """
            SELECT mutation_id, flow_id, work_id, status, op_json, attempts, error
            FROM continue_mutations
            WHERE mutation_id = ?
            """,
            (mutation_id,),
        ).fetchone()
        if row is None:
            return None
        return MutationRow(
            mutation_id=str(row["mutation_id"]),
            flow_id=str(row["flow_id"]),
            work_id=str(row["work_id"]) if row["work_id"] is not None else None,
            status=str(row["status"]),
            op_json=str(row["op_json"]),
            attempts=int(row["attempts"]),
            error=str(row["error"]) if row["error"] is not None else None,
        )

    def set_mutation_status(self, mutation_id: str, *, status: str, error: Optional[str] = None) -> None:
        self._conn.execute(
            """
            UPDATE continue_mutations
            SET status = ?, error = ?, attempts = attempts + 1, updated_at = ?
            WHERE mutation_id = ?
            """,
            (status, error, self._now(), mutation_id),
        )

    def list_pending_mutations(
        self, *, flow_id: Optional[str] = None, limit: int = 100,
    ) -> list[MutationRow]:
        if flow_id:
            rows = self._conn.execute(
                """
                SELECT mutation_id, flow_id, work_id, status, op_json, attempts, error
                FROM continue_mutations
                WHERE status = 'pending' AND flow_id = ?
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (flow_id, max(int(limit), 1)),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT mutation_id, flow_id, work_id, status, op_json, attempts, error
                FROM continue_mutations
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (max(int(limit), 1),),
            ).fetchall()
        return [
            MutationRow(
                mutation_id=str(row["mutation_id"]),
                flow_id=str(row["flow_id"]),
                work_id=str(row["work_id"]) if row["work_id"] is not None else None,
                status=str(row["status"]),
                op_json=str(row["op_json"]),
                attempts=int(row["attempts"]),
                error=str(row["error"]) if row["error"] is not None else None,
            )
            for row in rows
        ]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
