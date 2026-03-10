import json
import sqlite3

from keep.work_store import SQLiteFlowStore


def test_sqlite_flow_store_transaction_rollback(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"cursor": {"step": 0}}))
        store.rollback()
        assert store.get_flow(flow.flow_id) is None
    finally:
        store.close()


def test_sqlite_flow_store_flow_work_and_idempotency(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"cursor": {"step": 0}}))
        work_id = store.insert_work(
            flow_id=flow.flow_id,
            kind="summarize",
            input_json=json.dumps({"item_id": "note:1", "content": "alpha"}),
            output_contract_json=json.dumps({"must_return": ["summary"]}),
        )
        store.update_work_result(
            work_id=work_id,
            status="completed",
            result_json=json.dumps({"outputs": {"summary": "alpha"}}),
        )
        store.update_flow(
            flow.flow_id,
            state_version=1,
            status="done",
            state_json=json.dumps({"cursor": {"step": 1}}),
        )
        store.store_idempotent("idem-1", "hash-1", json.dumps({"status": "done"}))
        store.commit()

        loaded = store.get_flow(flow.flow_id)
        assert loaded is not None
        assert loaded.state_version == 1
        assert loaded.status == "done"
        assert store.has_any_work_key(flow.flow_id, "summarize") is True
        assert store.has_completed_work_key(flow.flow_id, "summarize") is True
        idem = store.load_idempotent("idem-1")
        assert idem == ("hash-1", json.dumps({"status": "done"}))
    finally:
        store.close()


def test_sqlite_flow_store_mutation_queue(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"cursor": {"step": 0}}))
        op_json = json.dumps(
            {"op": "set_tags", "target": "note:1", "tags": {"topic": "x"}},
            sort_keys=True,
            separators=(",", ":"),
        )
        mutation_id = store.insert_pending_mutation(
            flow_id=flow.flow_id,
            work_id="w_1",
            op_json=op_json,
        )
        store.commit()

        pending = store.list_pending_mutations(flow_id=flow.flow_id)
        assert [m.mutation_id for m in pending] == [mutation_id]
        store.set_mutation_status(mutation_id, status="applied")
        assert store.list_pending_mutations(flow_id=flow.flow_id) == []
    finally:
        store.close()


def test_sqlite_flow_store_claim_and_renew_lease(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"cursor": {"step": 0}}))
        work_a = store.insert_work(
            flow_id=flow.flow_id,
            kind="summarize",
            input_json=json.dumps({"item_id": "note:1", "content": "alpha"}),
            output_contract_json=json.dumps({"must_return": ["summary"]}),
        )
        work_b = store.insert_work(
            flow_id=flow.flow_id,
            kind="tag",
            input_json=json.dumps({"item_id": "note:2", "content": "beta"}),
            output_contract_json=json.dumps({"must_return": ["tags"]}),
        )
        store.commit()

        claimed_a = store.claim_requested_work(worker_id="worker-a", limit=1, lease_seconds=90)
        assert [row.work_id for row in claimed_a] == [work_a]
        assert claimed_a[0].claimed_by == "worker-a"
        assert claimed_a[0].lease_until is not None

        claimed_b = store.claim_requested_work(worker_id="worker-b", limit=10)
        assert [row.work_id for row in claimed_b] == [work_b]
        assert claimed_b[0].claimed_by == "worker-b"

        assert store.renew_work_lease(work_id=work_a, worker_id="worker-b") is False
        assert store.renew_work_lease(work_id=work_a, worker_id="worker-a") is True
    finally:
        store.close()


def test_sqlite_flow_store_retry_backoff_and_reclaim(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"cursor": {"step": 0}}))
        work_id = store.insert_work(
            flow_id=flow.flow_id,
            kind="summarize",
            input_json=json.dumps({"item_id": "note:1", "content": "alpha"}),
            output_contract_json=json.dumps({"must_return": ["summary"]}),
        )
        store.commit()

        claimed = store.claim_requested_work(worker_id="worker-a", limit=1)
        assert [row.work_id for row in claimed] == [work_id]

        released = store.release_work_for_retry(
            work_id=work_id,
            worker_id="worker-a",
            error="transient error",
            backoff_base_seconds=1,
            backoff_max_seconds=1,
        )
        assert released is True
        after_release = store.get_work(flow.flow_id, work_id)
        assert after_release is not None
        assert after_release.claimed_by is None
        assert after_release.retry_after is not None
        assert after_release.last_error == "transient error"
        assert after_release.attempt == 2

        # Simulate time passing to make work claimable again.
        store._conn.execute(
            "UPDATE continue_work SET retry_after = NULL WHERE work_id = ?",
            (work_id,),
        )
        reclaimed = store.claim_requested_work(worker_id="worker-b", limit=1)
        assert [row.work_id for row in reclaimed] == [work_id]
        assert reclaimed[0].claimed_by == "worker-b"
    finally:
        store.close()


def test_sqlite_flow_store_dead_letters_when_attempts_exhausted(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"cursor": {"step": 0}}))
        work_id = store.insert_work(
            flow_id=flow.flow_id,
            kind="summarize",
            input_json=json.dumps({"item_id": "note:1", "content": "alpha"}),
            output_contract_json=json.dumps({"must_return": ["summary"]}),
        )
        store.commit()

        # Force immediate exhaustion on first failure.
        store._conn.execute(
            "UPDATE continue_work SET max_attempts = 1 WHERE work_id = ?",
            (work_id,),
        )
        claimed = store.claim_requested_work(worker_id="worker-a", limit=1)
        assert [row.work_id for row in claimed] == [work_id]

        released = store.release_work_for_retry(
            work_id=work_id,
            worker_id="worker-a",
            error="fatal",
        )
        assert released is True
        row = store.get_work(flow.flow_id, work_id)
        assert row is not None
        assert row.status == "dead_letter"
        assert row.dead_lettered_at is not None
        assert row.last_error == "fatal"

        assert store.claim_requested_work(worker_id="worker-b", limit=10) == []
    finally:
        store.close()


def test_sqlite_flow_store_migrates_continue_work_claim_columns(tmp_path):
    db_path = tmp_path / "continuation.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("""
            CREATE TABLE continue_work (
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
        conn.commit()
    finally:
        conn.close()

    store = SQLiteFlowStore(db_path)
    try:
        columns = {
            str(row["name"])
            for row in store._conn.execute("PRAGMA table_info(continue_work)").fetchall()
        }
        for required in {
            "claimed_by",
            "claimed_at",
            "lease_until",
            "retry_after",
            "last_error",
            "max_attempts",
            "dead_lettered_at",
        }:
            assert required in columns
    finally:
        store.close()


def test_supersede_marks_older_unclaimed_work(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"step": 0}))
        store.commit()

        key = "analyze:doc1"
        # Insert two work items with the same supersede key
        w1 = store.insert_work(
            flow_id=flow.flow_id, kind="analyze",
            input_json='{"item_id":"doc1"}', output_contract_json="{}",
            supersede_key=key,
        )
        w2 = store.insert_work(
            flow_id=flow.flow_id, kind="analyze",
            input_json='{"item_id":"doc1"}', output_contract_json="{}",
            supersede_key=key,
        )

        # Supersede prior work — should mark w1
        count = store.supersede_prior_work(key, w2)
        assert count == 1

        row1 = store.get_work(flow.flow_id, w1)
        row2 = store.get_work(flow.flow_id, w2)
        assert row1.status == "superseded"
        assert row2.status == "requested"

        # Only w2 should count as pending
        assert store.count_requested_work() == 1
    finally:
        store.close()


def test_has_superseding_work_detects_newer(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"step": 0}))
        store.commit()

        key = "tag:doc1"
        w1 = store.insert_work(
            flow_id=flow.flow_id, kind="tag",
            input_json='{"item_id":"doc1"}', output_contract_json="{}",
            supersede_key=key,
        )
        w2 = store.insert_work(
            flow_id=flow.flow_id, kind="tag",
            input_json='{"item_id":"doc1"}', output_contract_json="{}",
            supersede_key=key,
        )

        row1 = store.get_work(flow.flow_id, w1)
        row2 = store.get_work(flow.flow_id, w2)
        # w1 has a newer sibling (w2)
        assert store.has_superseding_work(w1, key, row1.created_at) is True
        # w2 has no newer sibling
        assert store.has_superseding_work(w2, key, row2.created_at) is False
    finally:
        store.close()


def test_supersede_does_not_affect_claimed_work(tmp_path):
    store = SQLiteFlowStore(tmp_path / "continuation.db")
    try:
        store.begin_immediate()
        flow = store.create_flow(json.dumps({"step": 0}))
        store.commit()

        key = "analyze:doc1"
        w1 = store.insert_work(
            flow_id=flow.flow_id, kind="analyze",
            input_json='{"item_id":"doc1"}', output_contract_json="{}",
            supersede_key=key,
        )
        # Claim w1
        store.claim_requested_work(worker_id="test", limit=1, lease_seconds=60)

        # Insert w2 and try to supersede
        w2 = store.insert_work(
            flow_id=flow.flow_id, kind="analyze",
            input_json='{"item_id":"doc1"}', output_contract_json="{}",
            supersede_key=key,
        )
        count = store.supersede_prior_work(key, w2)
        # w1 is claimed, should not be superseded
        assert count == 0

        row1 = store.get_work(flow.flow_id, w1)
        assert row1.status == "requested"  # still requested (claimed)
    finally:
        store.close()
