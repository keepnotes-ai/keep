"""Background processing mixin for keep API.

Extracts task dispatch, processing pipeline, and process spawning methods
from the main Keep class into a composable mixin.  All method bodies are
identical to their originals in api.py.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .processors import _content_hash
from .types import (
    casefold_tags,
    casefold_tags_for_index,
    filter_non_system_tags,
    normalize_id,
    tag_values,
    utc_now,
    SYSTEM_TAG_PREFIX,
)

logger = logging.getLogger(__name__)



def _size_priority_bump(content_len: int) -> int:
    """Return a priority bump based on content length.

    Larger content takes longer to process, so it gets deprioritized
    (higher number = lower priority in a min-heap):
    - > 50KB → +2
    - > 10KB → +1
    - otherwise → 0
    """
    if content_len > 50_000:
        return 2
    if content_len > 10_000:
        return 1
    return 0


class BackgroundProcessingMixin:
    """Task dispatch, processing pipeline, and processor spawning.

    Requires the composing class to provide:
    - _config: StoreConfig
    - _is_local: bool
    - _store_path: Path
    - _store: VectorStoreProtocol
    - _document_store: DocumentStoreProtocol
    - _pending_queue: PendingQueueProtocol
    - _task_client: optional TaskClient (may be None)
    - _planner_stats: optional PlannerStatsStore (may be None)
    - _work_queue: optional WorkQueue slot (initially None)
    - _work_queue_lock: threading.Lock
    - _write_context_by_id: dict[str, dict[str, Any]]
    - _write_context_lock: threading.Lock
    - _last_spawn_time: float
    - _reconcile_done: threading.Event
    - _resolve_doc_collection(): str
    - _resolve_chroma_collection(): str
    - _get_embedding_provider(): EmbeddingProvider
    - _get_summarization_provider(): SummarizationProvider
    - _release_embedding_provider(): None
    - _try_dedup_embedding(...): optional embedding
    - _gather_context(item_id, user_tags): str
    - _gather_analyze_chunks(item_id, doc): list
    - _gather_guide_context(tags): str
    - _resolve_prompt_doc(task_type, tags): optional str
    """

    # -------------------------------------------------------------------------
    # Cluster 1: Task dispatch
    # -------------------------------------------------------------------------

    def _task_idempotency_key(
        self,
        *,
        task_type: str,
        id: str,
        content: str,
        metadata: Optional[dict[str, Any]] = None,
        tags: Optional[dict[str, Any]] = None,
    ) -> str:
        metadata_material = json.dumps(
            metadata or {},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        tag_material = json.dumps(
            casefold_tags(filter_non_system_tags(tags or {})),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(f"{metadata_material}|{tag_material}".encode("utf-8")).hexdigest()[:12]
        return f"bg:{task_type}:{id}:{_content_hash(content)}:{digest}"

    def _enqueue_flow_cursor(
        self,
        *,
        state: str,
        cursor_token: str,
        params: dict[str, Any] | None = None,
        priority: int = 5,
    ) -> None:
        """Enqueue a flow cursor to the work queue for daemon execution.

        Called when a foreground flow encounters an async action and
        produces a cursor for background resumption.
        """
        wq = self._get_work_queue()
        if wq is None:
            return
        wq.enqueue(
            "flow",
            {
                "state": state,
                "cursor": cursor_token,
                "params": params or {},
            },
            supersede_key=f"flow:{state}:{cursor_token[:32]}",
            priority=priority,
        )
        self._spawn_processor()

    def _enqueue_task_background(
        self,
        *,
        task_type: str,
        id: str,
        doc_coll: str,
        content: str,
        metadata: Optional[dict[str, Any]] = None,
        tags: Optional[dict[str, Any]] = None,
    ) -> None:
        """Enqueue a background task to the work queue.

        There is exactly one processing path for background tasks:
        work queue -> process_work_batch -> task_workflows.  Do NOT
        fall back to the pending queue -- that creates a duplicate
        execution path.
        """
        wq = self._get_work_queue()
        if wq is None:
            return  # No work queue available
        meta = dict(metadata or {})
        supersede_key = self._task_idempotency_key(
            task_type=task_type, id=id, content=content,
            metadata=metadata, tags=tags,
        )
        from .actions import get_action_priority
        priority = min(get_action_priority(task_type) + _size_priority_bump(len(content)), 9)
        wq.enqueue(
            task_type,
            {
                "task_type": task_type,
                "item_id": id,
                "collection": doc_coll,
                "content": content,
                "metadata": meta,
            },
            supersede_key=supersede_key,
            priority=priority,
        )

    def _dispatch_after_write_flow(
        self,
        *,
        item_id: str,
        content: str,
        uri: str = "",
        content_type: str = "",
        tags: Optional[dict[str, str]] = None,
        summary: Optional[str] = None,
        ocr_pages: Optional[list[int]] = None,
        doc_links: Optional[list[str]] = None,
    ) -> None:
        """Enqueue the after-write flow for background execution.

        The after-write state doc is the sole source of truth for what
        background work runs after a write.  This method enqueues a single
        flow work item; the daemon runs the flow via ``run_flow()`` with
        all actions executing through the standard flow runtime.

        System docs (dot-prefix IDs) are skipped entirely — they are
        authored reference material that needs no background processing.
        """
        if item_id.startswith("."):
            return

        wq = self._get_work_queue()
        if wq is None:
            return

        # Build item context — passed as flow params so the state doc's
        # CEL predicates can evaluate without re-reading the item.
        all_tags: dict[str, Any] = dict(tags or {})
        if ocr_pages:
            all_tags["_ocr_pages"] = str(ocr_pages)

        item_metadata: dict[str, Any] = {}
        if uri:
            item_metadata["uri"] = uri
        if content_type:
            item_metadata["content_type"] = content_type
        if ocr_pages:
            item_metadata["ocr_pages"] = list(ocr_pages)
        if doc_links:
            item_metadata["doc_links"] = list(doc_links)

        flow_params: dict[str, Any] = {
            "item_id": item_id,
            "item": {
                "content_length": len(content),
                "has_summary": bool(summary),
                "has_uri": bool(uri),
                "uri": uri or "",
                "content_type": content_type or "",
                "is_system_note": item_id.startswith("."),
                "tags": all_tags,
                "has_media_content": bool(
                    content_type and not content_type.startswith("text/")
                ),
                "has_content": bool(content),
            },
            "max_summary_length": self._config.max_summary_length,
            "system": {
                "has_media_provider": self._config.media is not None,
            },
            "metadata": item_metadata,
        }

        wq.enqueue(
            "flow",
            {
                "state": "after-write",
                "params": flow_params,
                "item_id": item_id,
                "content": content,
            },
            supersede_key=f"flow:after-write:{item_id}",
            priority=3,
        )

        # Write version file so the daemon can detect upgrades
        try:
            from . import __version__
            (self._store_path / ".processor.version").write_text(__version__)
        except Exception:
            pass
        self._spawn_processor()

    def _load_after_write_state_doc(self) -> Optional["StateDoc"]:
        """Load the after-write state doc with fragment composition."""
        return self._load_state_doc("after-write")

    def _load_state_doc(self, name: str) -> Optional["StateDoc"]:
        """Load a state doc by name, with fragment composition."""
        from .state_doc import load_state_doc

        try:
            doc_coll = self._resolve_doc_collection()
        except Exception:
            doc_coll = None

        def _get_note(id: str):
            if doc_coll is None:
                return None
            return self._document_store.get(doc_coll, id)

        def _list_children(prefix: str):
            if doc_coll is None:
                return []
            return self._document_store.query_by_id_prefix(doc_coll, prefix)

        return load_state_doc(name, get_note=_get_note, list_children=_list_children)

    def _store_write_context(self, item_id: str, context: dict[str, Any]) -> None:
        note_id = normalize_id(item_id)
        with self._write_context_lock:
            self._write_context_by_id[note_id] = dict(context)
            # Keep memory bounded for bursty writes.
            if len(self._write_context_by_id) > 256:
                oldest = next(iter(self._write_context_by_id))
                self._write_context_by_id.pop(oldest, None)

    def _consume_write_context(self, item_id: str) -> Optional[dict[str, Any]]:
        note_id = normalize_id(item_id)
        with self._write_context_lock:
            ctx = self._write_context_by_id.pop(note_id, None)
        if not isinstance(ctx, dict):
            return None
        return dict(ctx)

    # -------------------------------------------------------------------------
    # Cluster 2: Processing pipeline
    # -------------------------------------------------------------------------

    def _emit_processing_breakdown(
        self,
        *,
        item: Any,
        error: str,
        failure_class: str,
    ) -> None:
        logger.warning(
            "Processing breakdown for %s/%s (%s): %s",
            item.id, item.task_type, failure_class, error,
        )

    def process_pending(self, limit: int = 10, shutdown_check=None) -> dict:
        """Process pending work items (embedding, reindex, infrastructure).

        Handles task types serially:
        - "embed": computes and stores embeddings (cloud mode deferred writes)
        - "reindex": re-embeds after model/dimension change
        - "backfill-edges": populates inverse edge tags
        - "planner-rebuild": rebuilds planner statistics

        Summarize, analyze, OCR, describe, and tag tasks are handled by
        the work queue (process_pending_work) via task_workflows.
        If any appear here (legacy data), they are completed and skipped.

        Items that fail MAX_SUMMARY_ATTEMPTS times are removed from
        the queue.

        Args:
            limit: Maximum number of items to process in this batch

        Returns:
            Dict with: processed (int), failed (int), abandoned (int), errors (list)
        """
        from .processors import DELEGATABLE_TASK_TYPES

        items = self._pending_queue.dequeue(limit=limit)
        result = {"processed": 0, "failed": 0, "abandoned": 0, "delegated": 0, "errors": []}

        for item in items:
            if shutdown_check and shutdown_check():
                logger.info("Shutdown requested, stopping pending batch")
                break

            # Skip items that have failed too many times
            # (attempts was already incremented by dequeue, so check >= MAX)
            if item.attempts >= self._config.max_task_attempts:
                # Move to dead letter -- preserved for diagnosis
                self._pending_queue.abandon(
                    item.id, item.collection, item.task_type,
                    error=f"Exhausted {item.attempts} attempts",
                )
                self._emit_processing_breakdown(
                    item=item,
                    error=f"Exhausted {item.attempts} attempts",
                    failure_class="exhausted_attempts",
                )
                result["abandoned"] += 1
                logger.warning(
                    "Abandoned pending %s after %d attempts: %s",
                    item.task_type, item.attempts, item.id
                )
                continue

            # Delegate to hosted service if available and appropriate
            if (
                self._task_client
                and item.task_type in DELEGATABLE_TASK_TYPES
                and not (item.metadata or {}).get("_local_only")
            ):
                try:
                    self._delegate_task(item)
                    result["delegated"] += 1
                    continue
                except Exception as e:
                    logger.warning(
                        "Delegation failed for %s %s, falling back to local: %s",
                        item.task_type, item.id, e,
                    )
                    # Fall through to local processing

            try:
                _task_verbs = {
                    "backfill-edges": "Backfilling edges",
                    "embed": "Embedding",
                    "planner-rebuild": "Rebuilding planner stats",
                    "reindex": "Re-embedding",
                }
                verb = _task_verbs.get(item.task_type, item.task_type)
                logger.info("%s %s (attempt %d)", verb, item.id, item.attempts)
                if item.task_type in ("analyze", "ocr", "describe", "tag", "summarize"):
                    # These tasks belong on the work queue, not here.
                    # Complete and skip -- they'll be processed via
                    # process_pending_work / task_workflows.
                    logger.info(
                        "Skipping %s/%s on pending queue (handled by work queue)",
                        item.task_type, item.id,
                    )
                    self._pending_queue.complete(
                        item.id, item.collection, item.task_type
                    )
                    result["processed"] += 1
                    continue
                elif item.task_type == "backfill-edges":
                    self._process_pending_backfill_edges(item)
                elif item.task_type == "embed":
                    self._process_pending_embed(item)
                    self._release_embedding_provider()
                elif item.task_type == "planner-rebuild":
                    if self._planner_stats:
                        self._planner_stats.rebuild(
                            self._document_store, item.collection,
                        )
                elif item.task_type == "reindex":
                    self._process_pending_reindex(item)
                    self._release_embedding_provider()
                else:
                    logger.warning(
                        "Unknown task type %r for %s, completing",
                        item.task_type, item.id,
                    )

                # Remove from queue
                self._pending_queue.complete(
                    item.id, item.collection, item.task_type
                )
                result["processed"] += 1
                logger.info("%s %s done", verb, item.id)

                # Brief yield between items so interactive processes
                # (keep now, keep find) can acquire the model lock
                # without waiting for the entire batch to finish.
                time.sleep(0.1)

            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"

                # Permanent failures: content issues that won't resolve
                # on retry (e.g. scanned PDF with no text layer).
                _permanent = (
                    "content too short" in str(e).lower()
                    or "no text extracted" in str(e).lower()
                    or "no content extractor" in str(e).lower()
                )
                if _permanent:
                    self._pending_queue.abandon(
                        item.id, item.collection, item.task_type,
                        error=f"Permanent: {error_msg}",
                    )
                    self._emit_processing_breakdown(
                        item=item,
                        error=error_msg,
                        failure_class="permanent_failure",
                    )
                    result["abandoned"] = result.get("abandoned", 0) + 1
                    logger.info(
                        "Abandoned %s %s (permanent failure): %s",
                        item.task_type, item.id, e,
                    )
                    continue

                # Transient failure -- retry on next batch.
                # (attempt counter already incremented by dequeue)
                self._pending_queue.fail(
                    item.id, item.collection, item.task_type,
                    error=error_msg,
                )
                result["failed"] += 1
                result["errors"].append(f"{item.id}: {error_msg}")
                logger.warning("Failed to %s %s (attempt %d): %s",
                             item.task_type, item.id, item.attempts, e)

        # Drain planner outbox (bounded)
        if self._planner_stats:
            try:
                doc_coll = self._resolve_doc_collection()
                planner_result = self._planner_stats.drain_outbox(
                    self._document_store, doc_coll,
                    max_items=20, max_ms=200,
                )
                if planner_result["processed"]:
                    logger.info(
                        "Planner stats: processed=%d failed=%d",
                        planner_result["processed"], planner_result["failed"],
                    )
            except Exception as e:
                logger.debug("Planner drain skipped: %s", e)

        # Poll for delegated task results
        if self._task_client:
            self._poll_delegated(result)

        return result

    def _delegate_task(self, item) -> None:
        """Submit a task to the hosted service and mark as delegated."""
        from .task_client import TaskClientError

        content = item.content
        # For summarize tasks, gather context to include in metadata
        meta = dict(item.metadata or {})

        if item.task_type == "summarize":
            doc = self._document_store.get(item.collection, item.id)
            if doc:
                user_tags = filter_non_system_tags(doc.tags)
                if user_tags:
                    context = self._gather_context(item.id, user_tags)
                    if context:
                        meta["context"] = context
                # Resolve prompt from .prompt/summarize/* docs
                try:
                    prompt = self._resolve_prompt_doc("summarize", doc.tags)
                    if prompt:
                        meta["system_prompt_override"] = prompt
                except Exception:
                    pass  # Remote falls back to its default

        elif item.task_type == "analyze":
            doc = self._document_store.get(item.collection, item.id)
            if not doc:
                logger.info("Skipping delegation for deleted doc: %s", item.id)
                return
            # Gather chunks, guide context, and tag specs locally
            meta["chunks"] = self._gather_analyze_chunks(item.id, doc)
            guidance_tags = (item.metadata or {}).get("tags")
            if guidance_tags:
                meta["guide_context"] = self._gather_guide_context(guidance_tags)
            try:
                from .analyzers import TagClassifier
                classifier = TagClassifier(
                    provider=self._get_summarization_provider(),
                )
                specs = classifier.load_specs(self)
                if specs:
                    meta["tag_specs"] = specs
            except Exception as e:
                logger.warning("Could not load tag specs for delegation: %s", e)
            # Resolve prompt from .prompt/analyze/* docs
            try:
                prompt = self._resolve_prompt_doc("analyze", doc.tags)
                if prompt:
                    meta["prompt_override"] = prompt
            except Exception:
                pass  # Remote falls back to its default
            content = ""  # content is in chunks, not top-level

        try:
            remote_task_id = self._task_client.submit(
                item.task_type, content, meta or None,
            )
        except TaskClientError:
            raise  # Let caller handle fallback

        self._pending_queue.mark_delegated(
            item.id, item.collection, item.task_type, remote_task_id,
        )
        logger.info(
            "Delegated %s %s -> remote task %s",
            item.task_type, item.id, remote_task_id,
        )

    # Max time a task can stay delegated before falling back to local
    DELEGATION_STALE_SECONDS = 3600  # 1 hour

    def _poll_delegated(self, result: dict) -> None:
        """Poll for results of delegated tasks and apply them."""
        from .processors import ProcessorResult
        from .task_client import TaskClientError

        delegated = self._pending_queue.list_delegated()
        if not delegated:
            return

        now = datetime.now(timezone.utc)

        for item in delegated:
            remote_task_id = item.metadata.get("_remote_task_id")
            if not remote_task_id:
                continue

            try:
                resp = self._task_client.poll(remote_task_id)
            except TaskClientError as e:
                logger.warning("Poll failed for %s: %s", remote_task_id, e)
                # Check staleness even on poll failure
                if self._is_delegation_stale(item, now):
                    self._revert_stale_delegation(item, result,
                                                  "Poll unreachable and delegation stale")
                continue

            if resp["status"] == "completed":
                task_result = resp.get("result") or {}
                proc_result = ProcessorResult(
                    task_type=item.task_type,
                    summary=task_result.get("summary"),
                    content=task_result.get("content"),
                    content_hash=task_result.get("content_hash"),
                    content_hash_full=task_result.get("content_hash_full"),
                    parts=task_result.get("parts"),
                )
                # Get existing doc tags for apply_result
                doc = self._document_store.get(item.collection, item.id)
                existing_tags = doc.tags if doc else None
                self.apply_result(
                    item.id, item.collection, proc_result,
                    existing_tags=existing_tags,
                )
                self._pending_queue.complete(item.id, item.collection, item.task_type)
                try:
                    self._task_client.acknowledge(remote_task_id)
                except Exception:
                    pass  # Non-critical
                result["processed"] = result.get("processed", 0) + 1
                logger.info("Delegated %s %s completed", item.task_type, item.id)

            elif resp["status"] == "failed":
                error = resp.get("error", "Remote processing failed")
                self._pending_queue.fail(
                    item.id, item.collection, item.task_type,
                    error=f"Remote: {error}",
                )
                result["failed"] = result.get("failed", 0) + 1
                logger.warning(
                    "Delegated %s %s failed: %s",
                    item.task_type, item.id, error,
                )

            elif resp["status"] == "not_found":
                # Task disappeared from server (cleanup, bug, etc.)
                # Revert to pending for local processing
                self._revert_stale_delegation(item, result,
                                              "Remote task not found")

            else:
                # Still queued/processing -- check staleness
                if self._is_delegation_stale(item, now):
                    self._revert_stale_delegation(item, result,
                                                  "Delegation stale (>1h)")

    def _is_delegation_stale(self, item, now) -> bool:
        """Check if a delegated item has exceeded the staleness threshold."""
        if not item.delegated_at:
            return False
        try:
            delegated_time = datetime.fromisoformat(item.delegated_at)
            age = (now - delegated_time).total_seconds()
            return age > self.DELEGATION_STALE_SECONDS
        except (ValueError, TypeError):
            return False

    def _revert_stale_delegation(self, item, result: dict, reason: str) -> None:
        """Revert a delegated item to pending for local processing."""
        self._pending_queue.fail(
            item.id, item.collection, item.task_type,
            error=f"Delegation reverted: {reason}",
        )
        result["failed"] = result.get("failed", 0) + 1
        logger.warning(
            "Delegated %s %s reverted to local: %s",
            item.task_type, item.id, reason,
        )

    def apply_result(self, item_id, collection, result, *, existing_tags=None):
        """Apply a ProcessorResult to the local store.

        Converts the legacy ProcessorResult into a mutation list and
        delegates to the generic ``_apply_mutations`` applier.
        """
        from .task_workflows import _apply_mutations

        mutations: list[dict[str, Any]] = []

        if result.task_type == "summarize":
            mutations.append({
                "op": "set_summary", "target": item_id,
                "summary": result.summary or "",
            })

        elif result.task_type == "ocr":
            mutations.append({
                "op": "set_content", "target": item_id,
                "content": result.content or "",
                "summary": result.summary or "",
                "content_hash": result.content_hash or "",
                "content_hash_full": result.content_hash_full or "",
            })

        elif result.task_type == "describe":
            mutations.append({
                "op": "set_summary", "target": item_id,
                "summary": result.summary or "",
                "embed": True,
            })

        elif result.task_type == "analyze":
            doc = self._document_store.get(collection, item_id)
            if not doc:
                return

            mutations.append({"op": "delete_prefix", "prefix": f"{item_id}@p"})

            parent_user_tags = {
                k: v for k, v in (existing_tags or {}).items()
                if not k.startswith(SYSTEM_TAG_PREFIX)
            }
            for i, raw in enumerate(result.parts or [], 1):
                part_tags = dict(parent_user_tags)
                if raw.get("tags"):
                    part_tags.update(raw["tags"])
                part_tags["_base_id"] = item_id
                part_tags["_part_num"] = str(i)
                mutations.append({
                    "op": "put_item",
                    "id": f"{item_id}@p{i}",
                    "content": raw.get("content", ""),
                    "summary": raw.get("summary", ""),
                    "tags": part_tags,
                    "queue_background_tasks": False,
                })

            if doc.content_hash:
                updated_tags = dict(doc.tags)
                updated_tags["_analyzed_hash"] = doc.content_hash
                versions = self._document_store.list_versions(collection, item_id, limit=1)
                if versions:
                    updated_tags["_analyzed_version"] = str(versions[0].version)
                mutations.append({
                    "op": "set_tags", "target": item_id,
                    "tags": updated_tags,
                })

        if mutations:
            _apply_mutations(self, collection, {"mutations": mutations})

    def _run_local_task_workflow(
        self,
        *,
        task_type: str,
        item_id: str,
        collection: str,
        content: str,
        metadata: Optional[dict] = None,
    ) -> dict:
        """Run shared local workflow for summarize/ocr/analyze/tag tasks.

        Returns a small status envelope:
        {"status": "applied|skipped", "details": {...}}
        """
        from .task_workflows import TaskRequest, run_local_task

        outcome = run_local_task(
            self,
            TaskRequest(
                task_type=task_type,
                id=item_id,
                collection=collection,
                content=content,
                metadata=dict(metadata or {}),
            ),
        )
        return {"status": outcome.status, "details": dict(outcome.details)}

    def _process_pending_embed(self, item) -> None:
        """Process a deferred embedding task (cloud mode).

        Computes the embedding for the content and writes it to the
        vector store.  If the doc's content changed (metadata flag),
        archives the old embedding as a versioned entry first.
        """
        doc_coll = item.collection
        chroma_coll = self._resolve_chroma_collection()

        # Get current doc record (may have been deleted before we got here)
        doc = self._document_store.get(doc_coll, item.id)
        if doc is None:
            return

        content_changed = (item.metadata or {}).get("content_changed", False)

        # Archive old embedding before overwriting (version archival)
        if content_changed:
            old_embedding = self._store.get_embedding(chroma_coll, item.id)
            max_ver = self._document_store.max_version(doc_coll, item.id)
            if max_ver > 0 and old_embedding is not None:
                # Get the archived version's metadata for the versioned entry
                archived = self._document_store.get_version(
                    doc_coll, item.id, offset=1
                )
                if archived:
                    self._store.upsert_version(
                        collection=chroma_coll,
                        id=item.id,
                        version=max_ver,
                        embedding=old_embedding,
                        summary=archived.summary,
                        tags=casefold_tags_for_index(archived.tags),
                    )

        # Compute embedding (try dedup first)
        embedding = self._try_dedup_embedding(
            doc_coll, chroma_coll, doc.content_hash, item.id, item.content,
        )
        if embedding is None:
            embedding = self._get_embedding_provider().embed(item.content)

        # Write to vector store
        self._store.upsert(
            collection=chroma_coll,
            id=item.id,
            embedding=embedding,
            summary=doc.summary,
            tags=casefold_tags_for_index(doc.tags),
        )

    def _process_pending_reindex(self, item) -> None:
        """Process a reindex task: embed summary and write to vector store.

        Handles both main docs and versioned entries.
        The item.content contains the summary text to embed.
        """
        if not item.content or not item.content.strip():
            logger.info("Skipping reindex for %s: no summary to embed", item.id)
            return

        chroma_coll = self._resolve_chroma_collection()
        meta = item.metadata or {}
        version = meta.get("version")
        base_id = meta.get("base_id")

        embedding = self._get_embedding_provider().embed(item.content)

        if version is not None and base_id is not None:
            # Versioned entry
            self._store.upsert_version(
                collection=chroma_coll,
                id=base_id,
                version=version,
                embedding=embedding,
                summary=item.content,
                tags=casefold_tags_for_index(meta.get("tags", {})),
            )
        else:
            # Main doc entry -- use fresh tags from doc store
            doc = self._document_store.get(item.collection, item.id)
            if doc is None:
                return  # deleted since enqueue
            self._store.upsert(
                collection=chroma_coll,
                id=item.id,
                embedding=embedding,
                summary=item.content,
                tags=casefold_tags_for_index(doc.tags),
            )

    def _flush_edge_backfill(self, doc_coll: str) -> None:
        """Synchronously backfill edges for tag docs with _inverse.

        Called from find(deep=True) after system-doc migration so that
        newly-created tag definitions produce edges before the search
        runs.  Finds incomplete backfills and runs them inline.
        """
        # Find tag docs with _inverse that haven't been backfilled yet
        tag_docs = self._document_store.query_by_id_prefix(doc_coll, ".tag/")
        for td in tag_docs:
            inverse = td.tags.get("_inverse")
            if not inverse:
                continue
            if isinstance(inverse, list):
                inverse = inverse[0]
            predicate = td.id[5:]  # strip ".tag/"
            if "/" in predicate:
                continue  # sub-tags like .tag/act/commitment
            if self._document_store.get_backfill_status(doc_coll, predicate):
                continue
            # Build a synthetic pending item for the backfill processor
            from .pending_summaries import PendingSummary
            synthetic = PendingSummary(
                id=f"_backfill:{predicate}",
                collection=doc_coll,
                content="",
                queued_at="",
                attempts=1,
                task_type="backfill-edges",
                metadata={"predicate": predicate, "inverse": inverse},
            )
            try:
                logger.info("Inline edge backfill: %s -> %s", predicate, inverse)
                self._process_pending_backfill_edges(synthetic)
            except Exception as e:
                logger.warning("Edge backfill failed for %s: %s", predicate, e)

    def _process_pending_backfill_edges(self, item) -> None:
        """Process a backfill-edges task: populate edges for all docs with a given tag.

        Scans all documents that have the predicate tag and creates edge
        rows + auto-vivifies targets. Marks the backfill as completed.
        """
        meta = item.metadata or {}
        predicate = meta.get("predicate")
        inverse = meta.get("inverse")
        if not predicate or not inverse:
            logger.warning("Backfill-edges task missing predicate/inverse: %s", item.id)
            return

        doc_coll = item.collection

        # Verify the tagdoc still has this _inverse -- it may have been
        # removed or changed since this task was enqueued.
        tagdoc = self._document_store.get(doc_coll, f".tag/{predicate}")
        current_inverse = tagdoc.tags.get("_inverse") if tagdoc else None
        if current_inverse != inverse:
            logger.info(
                "Backfill for %s skipped: _inverse changed (%s -> %s)",
                predicate, inverse, current_inverse,
            )
            # Clean up stale backfill record
            self._document_store.delete_backfill(doc_coll, predicate)
            return

        # Paginate -- query_by_tag_key defaults to limit=100
        edge_count = 0
        page_size = 500
        offset = 0
        while True:
            docs = self._document_store.query_by_tag_key(
                doc_coll, predicate, limit=page_size, offset=offset,
            )
            if not docs:
                break
            offset += len(docs)
            for doc in docs:
                for raw_target in tag_values(doc.tags, predicate):
                    if not raw_target:
                        continue
                    try:
                        target_id = normalize_id(raw_target)
                    except ValueError:
                        logger.warning(
                            "Skipping invalid backfill edge target %r for %s:%s",
                            raw_target, doc.id, predicate,
                        )
                        continue
                    if target_id.startswith("."):
                        continue
                    # Auto-vivify target (doc store only -- embedding via reindex queue)
                    if not self._document_store.exists(doc_coll, target_id):
                        reference_created = (
                            doc.tags.get("_created")
                            or doc.tags.get("_updated")
                            or utc_now()
                        )
                        now = utc_now()
                        self._document_store.upsert(
                            doc_coll, target_id,
                            summary="",
                            tags={
                                "_created": reference_created,
                                "_updated": now,
                                "_source": "auto-vivify",
                            },
                            created_at=reference_created,
                        )
                        self._pending_queue.enqueue(
                            target_id, doc_coll, target_id,
                            task_type="reindex",
                            metadata={
                                "tags": {
                                    "_created": reference_created,
                                    "_updated": now,
                                    "_source": "auto-vivify",
                                },
                            },
                        )
                    created = doc.tags.get("_created") or doc.tags.get("_updated") or utc_now()
                    self._document_store.upsert_edge(
                        collection=doc_coll,
                        source_id=doc.id,
                        predicate=predicate,
                        target_id=target_id,
                        inverse=inverse,
                        created=created,
                    )
                    edge_count += 1
        # Materialize archived-version edges for this predicate too.
        self._document_store.backfill_version_edges_for_predicate(
            doc_coll, predicate, inverse,
        )
        # Mark backfill complete
        self._document_store.upsert_backfill(doc_coll, predicate, inverse, completed=utc_now())
        logger.info("Backfilled %d edges for predicate=%s inverse=%s", edge_count, predicate, inverse)

    def _ocr_image(self, path: Path, content_type: str, extractor) -> str | None:
        """OCR a single image file. Returns extracted text or None."""
        from .processors import ocr_image
        return ocr_image(path, content_type, extractor)

    def _ocr_pdf(self, path: Path, ocr_pages: list[int], extractor) -> str | None:
        """OCR scanned PDF pages and merge with text-layer pages."""
        from .processors import ocr_pdf
        return ocr_pdf(path, ocr_pages, extractor)

    def pending_count(self) -> int:
        """Get count of pending summaries awaiting processing."""
        return self._pending_queue.count()

    def pending_work_count(self, *, claimable_only: bool = False) -> int:
        """Get count of requested work items."""
        wq = self._get_work_queue()
        if wq is None:
            return 0
        return wq.count(claimable_only=claimable_only)

    def process_pending_work(
        self,
        *,
        limit: int = 10,
        worker_id: Optional[str] = None,
        lease_seconds: int = 120,
        shutdown_check=None,
    ) -> dict:
        """Process a batch of requested work items."""
        wq = self._get_work_queue()
        if wq is None:
            return {"claimed": 0, "processed": 0, "failed": 0, "dead_lettered": 0, "errors": []}
        from .work_processor import process_work_batch
        return process_work_batch(
            self,
            wq,
            limit=limit,
            worker_id=worker_id,
            shutdown_check=shutdown_check,
            lease_seconds=lease_seconds,
        )

    def pending_stats(self) -> dict:
        """Get pending summary queue statistics.

        Returns dict with: pending, collections, max_attempts, oldest, queue_path, by_type
        """
        return self._pending_queue.stats()

    def pending_stats_by_type(self) -> dict[str, int]:
        """Get pending queue counts grouped by task type."""
        return self._pending_queue.stats_by_type()

    def pending_status(self, id: str) -> Optional[dict]:
        """Get pending task status for a specific note.

        Returns dict with id, task_type, status, queued_at if the note
        has pending work, or None if no work is pending. Requires a
        queue implementation that supports get_status().
        """
        return self._pending_queue.get_status(id)

    # -------------------------------------------------------------------------
    # Work queue (direct task dispatch, replaces FlowEngine for bg tasks)
    # -------------------------------------------------------------------------

    def _get_work_queue(self):
        """Get the work queue, lazy-creating a local SQLite one if needed.

        Returns None in cloud mode when no work queue was injected via
        StoreBundle.  Callers must handle None.
        """
        queue = self._work_queue
        if queue is not None:
            return queue
        if not self._is_local:
            return None  # Cloud mode: no queue unless injected
        from .work_queue import WorkQueue
        with self._work_queue_lock:
            queue = self._work_queue
            if queue is None:
                queue = WorkQueue(self._store_path / "continuation.db")
                self._work_queue = queue
        return queue

    # -------------------------------------------------------------------------
    # Cluster 3: Process spawning
    # -------------------------------------------------------------------------

    @property
    def _processor_pid_path(self) -> Path:
        """Path to the processor PID file."""
        return self._store_path / "processor.pid"

    def _is_processor_running(self) -> bool:
        """Check if a processor is already running via lock probe."""
        from .model_lock import ModelLock

        lock = ModelLock(self._store_path / ".processor.lock")
        return lock.is_locked()

    def _spawn_processor(self) -> bool:
        """Spawn a background processor if not already running.

        Uses an exclusive file lock to prevent TOCTOU race conditions
        where two processes could both check, find no processor, and
        both spawn one.

        Throttled: skips if < 30s since last spawn.
        Gated: waits up to 5s for background reconcile to finish.

        Returns True if a new processor was spawned, False if one was
        already running or spawn failed.
        """
        if not self._is_local:
            return False  # Cloud mode: worker is external

        import time
        from .model_lock import ModelLock

        # Throttle: don't spawn more than once per 30 seconds
        now = time.monotonic()
        if now - self._last_spawn_time < 30:
            return False

        # Gate on reconcile: wait briefly, skip if not done
        if not self._reconcile_done.wait(timeout=5):
            logger.debug("Skipping spawn: reconcile still in progress")
            return False

        spawn_lock = ModelLock(self._store_path / ".processor_spawn.lock")

        # Non-blocking: if another process is already spawning, let it handle it
        if not spawn_lock.acquire(blocking=False):
            return False

        log_fd = None
        try:
            if self._is_processor_running():
                return False

            # Spawn detached process
            # Use sys.executable to ensure we use the same Python
            cmd = [
                sys.executable, "-m", "keep.cli",
                "pending",
                "--daemon",
                "--store", str(self._store_path),
            ]

            # Platform-specific detachment
            # Redirect daemon stderr to ops log for crash diagnostics
            log_path = self._store_path / "keep-ops.log"
            try:
                log_fd = open(log_path, "a")
            except OSError:
                log_fd = None
            kwargs: dict = {
                "stdout": subprocess.DEVNULL,
                "stderr": log_fd if log_fd else subprocess.DEVNULL,
                "stdin": subprocess.DEVNULL,
            }

            if sys.platform != "win32":
                # Unix: start new session to fully detach
                kwargs["start_new_session"] = True
            else:
                # Windows: use CREATE_NEW_PROCESS_GROUP
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

            # Inherit KEEP_VERBOSE so daemon logs at debug level when
            # the parent was started with -v / KEEP_VERBOSE=1.
            env = os.environ.copy()
            if os.environ.get("KEEP_VERBOSE"):
                env["KEEP_VERBOSE"] = "1"

            subprocess.Popen(cmd, env=env, **kwargs)
            self._last_spawn_time = time.monotonic()
            logger.info("Spawned background processor")

            # Write current version so the daemon can detect upgrades
            # and exec-restart itself with the new code.
            try:
                from . import __version__
                (self._store_path / ".processor.version").write_text(__version__)
            except Exception:
                pass

            return True

        except Exception as e:
            # Spawn failed - log for debugging, queue will be processed later
            logger.warning("Failed to spawn background processor: %s", e)
            return False
        finally:
            # Close parent's copy of the log fd (child inherited it)
            if log_fd:
                log_fd.close()
            spawn_lock.release()
