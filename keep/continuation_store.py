"""Continuation flow store adapters.

Defines the persistence boundary for continuation runtime state so the runtime
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
    """A continuation flow record."""

    flow_id: str
    state_version: int
    status: str
    state_json: str


@dataclass
class WorkRow:
    """A work item record within a continuation flow."""

    work_id: str
    flow_id: str
    kind: str
    status: str
    input_json: str
    output_contract_json: str
    attempt: int


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
    """Persistence boundary for continuation flow state."""

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
    def get_work(self, flow_id: str, work_id: str) -> Optional[WorkRow]: ...
    def has_any_work_key(self, flow_id: str, key: str) -> bool: ...
    def has_completed_work_key(self, flow_id: str, key: str) -> bool: ...
    def insert_work(
        self, *, flow_id: str, kind: str, input_json: str, output_contract_json: str,
    ) -> str: ...
    def update_work_result(self, *, work_id: str, status: str, result_json: str) -> None: ...

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
            SELECT work_id, flow_id, kind, status, input_json, output_contract_json, attempt
            FROM continue_work
            WHERE flow_id = ? AND status = 'requested'
            ORDER BY created_at ASC
            """,
            (flow_id,),
        ).fetchall()
        return [
            WorkRow(
                work_id=str(row["work_id"]),
                flow_id=str(row["flow_id"]),
                kind=str(row["kind"]),
                status=str(row["status"]),
                input_json=str(row["input_json"]),
                output_contract_json=str(row["output_contract_json"]),
                attempt=int(row["attempt"]),
            )
            for row in rows
        ]

    def get_work(self, flow_id: str, work_id: str) -> Optional[WorkRow]:
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
        return WorkRow(
            work_id=str(row["work_id"]),
            flow_id=str(row["flow_id"]),
            kind=str(row["kind"]),
            status=str(row["status"]),
            input_json=str(row["input_json"]),
            output_contract_json=str(row["output_contract_json"]),
            attempt=int(row["attempt"]),
        )

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
            "UPDATE continue_work SET status = ?, result_json = ?, updated_at = ? WHERE work_id = ?",
            (status, result_json, self._now(), work_id),
        )

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
