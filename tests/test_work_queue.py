"""Concurrency and lifecycle tests for WorkQueue."""

from __future__ import annotations

import threading
import time

from keep.work_queue import WorkQueue


def test_concurrent_claim_no_overlap(tmp_path):
    """Two claimers should never receive the same work item."""
    queue = WorkQueue(tmp_path / "work.db")
    try:
        total = 20
        for i in range(total):
            queue.enqueue("tag", {"item_id": f"doc-{i}", "content": f"content {i}"})

        claimed_by_thread: list[list[str]] = [[], []]
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)

        def worker(idx: int) -> None:
            try:
                barrier.wait(timeout=5)
                for _ in range(10):
                    batch = queue.claim(f"worker-{idx}", limit=2)
                    claimed_by_thread[idx].extend(item.work_id for item in batch)
                    for item in batch:
                        queue.complete(item.work_id, {"status": "ok"})
            except BaseException as exc:  # pragma: no cover - surfaced by assertions
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(0,)),
            threading.Thread(target=worker, args=(1,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert not errors, [repr(e) for e in errors]
        all_claimed = claimed_by_thread[0] + claimed_by_thread[1]
        assert len(all_claimed) == total
        assert len(all_claimed) == len(set(all_claimed))
        assert queue.count() == 0
    finally:
        queue.close()


def test_concurrent_producers_and_consumers_do_not_raise_sqlite_errors(tmp_path):
    """Concurrent enqueue/claim/complete should not hit SQLite transaction errors."""
    queue = WorkQueue(tmp_path / "work.db")
    try:
        producers = 2
        items_per_producer = 15
        total = producers * items_per_producer

        start_barrier = threading.Barrier(4)
        producers_done = threading.Event()
        errors: list[BaseException] = []
        seen_work_ids: set[str] = set()
        seen_lock = threading.Lock()
        processed = 0
        processed_lock = threading.Lock()

        def producer(idx: int) -> None:
            try:
                start_barrier.wait(timeout=5)
                for i in range(items_per_producer):
                    queue.enqueue(
                        "tag",
                        {"item_id": f"p{idx}-doc-{i}", "content": f"content {idx}-{i}"},
                    )
                    time.sleep(0.001)
            except BaseException as exc:  # pragma: no cover - surfaced by assertions
                errors.append(exc)

        def consumer(idx: int) -> None:
            nonlocal processed
            try:
                start_barrier.wait(timeout=5)
                while True:
                    batch = queue.claim(f"consumer-{idx}", limit=2)
                    if not batch:
                        if producers_done.is_set() and queue.count() == 0:
                            break
                        time.sleep(0.002)
                        continue
                    for item in batch:
                        with seen_lock:
                            assert item.work_id not in seen_work_ids
                            seen_work_ids.add(item.work_id)
                        queue.complete(item.work_id, {"status": "ok"})
                        with processed_lock:
                            processed += 1
            except BaseException as exc:  # pragma: no cover - surfaced by assertions
                errors.append(exc)

        producer_threads = [
            threading.Thread(target=producer, args=(0,)),
            threading.Thread(target=producer, args=(1,)),
        ]
        consumer_threads = [
            threading.Thread(target=consumer, args=(0,)),
            threading.Thread(target=consumer, args=(1,)),
        ]

        for thread in producer_threads + consumer_threads:
            thread.start()
        for thread in producer_threads:
            thread.join(timeout=10)
        producers_done.set()
        for thread in consumer_threads:
            thread.join(timeout=10)

        assert not errors, [repr(e) for e in errors]
        assert processed == total
        assert len(seen_work_ids) == total
        assert queue.count() == 0
    finally:
        queue.close()
