"""Work processor — claims and executes queued work items.

Replaces FlowEngine's process_requested_work_batch with direct
task_workflows dispatch.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from .work_queue import WorkQueue

if TYPE_CHECKING:
    from .api import Keeper

logger = logging.getLogger(__name__)


def process_work_batch(
    keeper: "Keeper",
    queue: WorkQueue,
    *,
    limit: int = 10,
    worker_id: Optional[str] = None,
    lease_seconds: int = 120,
    shutdown_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Claim and execute a batch of work items.

    Returns a summary dict compatible with the old FlowEngine interface.
    """
    worker = worker_id or f"local-daemon:{os.getpid()}"
    items = queue.claim(worker, limit=limit, lease_seconds=lease_seconds)

    stats: dict[str, int] = {"claimed": len(items), "processed": 0, "failed": 0, "dead_lettered": 0}
    errors: list[dict[str, str]] = []

    for item in items:
        if shutdown_check and shutdown_check():
            logger.info("Shutdown requested, stopping work batch")
            break

        # Skip if superseded by a newer item
        if (
            item.supersede_key
            and item.created_at
            and queue.has_superseding(item.work_id, item.supersede_key, item.created_at)
        ):
            queue.complete(item.work_id, {"status": "superseded"})
            stats["processed"] += 1
            continue

        target = item.input.get("item_id") or item.input.get("id") or "?"
        try:
            logger.info("Processing %s %s", item.kind, target)
            outcome = _execute_work_item(keeper, item.kind, item.input)
            status = outcome.get("status", "applied")
            details = outcome.get("details")
            queue.complete(item.work_id, outcome)
            if status == "skipped":
                logger.info("Skipped %s %s: %s", item.kind, target, details or "")
            else:
                logger.info("Done %s %s", item.kind, target)
            stats["processed"] += 1
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            logger.warning("Failed %s %s: %s", item.kind, target, msg)
            queue.fail(item.work_id, worker, msg)
            stats["failed"] += 1
            errors.append({"work_id": item.work_id, "error": msg})

    stats["errors"] = errors  # type: ignore[assignment]

    # Log perf summary + queue depth after each batch with actual work
    if stats["processed"] or stats["failed"]:
        from .perf_stats import perf
        perf.log_summary()
        remaining = queue.count()
        if remaining > 0:
            by_kind = queue.count_by_kind()
            parts = [f"{v} {k}" for k, v in sorted(by_kind.items(), key=lambda x: -x[1])]
            logger.info("Queue: %d remaining (%s)", remaining, ", ".join(parts))

    return stats


def _execute_work_item(
    keeper: "Keeper",
    kind: str,
    input_data: dict[str, Any],
) -> dict[str, Any]:
    """Execute a single work item via keeper._run_local_task_workflow.

    Returns the outcome dict (keys: status, details).
    """
    # Special-case: git ingest is directory-level, not item-scoped
    if kind == "ingest_git":
        from .git_ingest import ingest_git_history
        directory = input_data.get("directory")
        if not directory:
            raise ValueError("ingest_git requires 'directory' in input")
        result = ingest_git_history(keeper, Path(directory))
        return {"status": "applied", "details": result}

    task_type = input_data.get("task_type") or kind
    item_id = str(input_data.get("item_id") or input_data.get("id") or "").strip()
    if not item_id:
        raise ValueError(f"work item missing item_id (kind={kind})")

    collection = str(
        input_data.get("collection")
        or input_data.get("doc_coll")
        or keeper._resolve_doc_collection()
    )
    content = str(input_data.get("content") or "")
    metadata = input_data.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    outcome = keeper._run_local_task_workflow(
        task_type=task_type,
        item_id=item_id,
        collection=collection,
        content=content,
        metadata=metadata,
    )
    return outcome or {"status": "applied"}
