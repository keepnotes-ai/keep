"""Shared local task workflows for processor-backed tasks.

These workflows encapsulate the current "magic behavior" for summarize/ocr/analyze
so queue-driven processing and continuation-driven processing can reuse the same
execution logic without duplication.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .processors import process_ocr, process_summarize
from .types import filter_non_system_tags

if TYPE_CHECKING:
    from .api import Keeper

logger = logging.getLogger(__name__)


@dataclass
class TaskRequest:
    """Minimal task request shape shared by queue and continuation paths."""

    task_type: str
    id: str
    collection: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRunResult:
    """Outcome of a local task workflow."""

    status: str  # "applied" | "skipped"
    details: dict[str, Any] = field(default_factory=dict)


def _run_summarize(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    doc = keeper._document_store.get(req.collection, req.id)
    if doc is None:
        return TaskRunResult(status="skipped", details={"reason": "deleted"})

    summarize_prompt = None
    try:
        summarize_prompt = keeper._resolve_prompt_doc("summarize", doc.tags)
    except Exception as e:
        logger.debug("Summarize prompt doc resolution failed: %s", e)

    context = None
    user_tags = filter_non_system_tags(doc.tags)
    if user_tags:
        context = keeper._gather_context(req.id, user_tags)

    result = process_summarize(
        req.content,
        context=context,
        summarization_provider=keeper._get_summarization_provider(),
        system_prompt_override=summarize_prompt,
    )
    keeper.apply_result(req.id, req.collection, result)
    return TaskRunResult(status="applied")


def _run_analyze(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    tags = req.metadata.get("tags")
    force = req.metadata.get("force", False)
    parts = keeper.analyze(req.id, tags=tags, force=force)
    return TaskRunResult(status="applied", details={"parts_count": len(parts)})


def _run_ocr(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    uri = req.metadata.get("uri") or req.id
    ocr_pages = req.metadata.get("ocr_pages", [])
    content_type = req.metadata.get("content_type", "")

    if not ocr_pages:
        return TaskRunResult(status="skipped", details={"reason": "no_ocr_pages"})

    max_ocr_pages = 1000
    if len(ocr_pages) > max_ocr_pages:
        logger.warning(
            "OCR page count %d exceeds limit %d for %s — truncating",
            len(ocr_pages), max_ocr_pages, uri,
        )
        ocr_pages = ocr_pages[:max_ocr_pages]

    extractor = keeper._get_content_extractor()
    if not extractor:
        raise IOError("No content extractor configured for OCR")

    path = Path(str(uri).removeprefix("file://")).resolve()
    if not path.exists():
        logger.warning("File no longer exists for OCR: %s", path)
        return TaskRunResult(status="skipped", details={"reason": "missing_file"})

    from .paths import validate_path_within_home

    try:
        validate_path_within_home(path)
    except ValueError:
        logger.warning("OCR path outside home directory, skipping: %s", path)
        return TaskRunResult(status="skipped", details={"reason": "path_outside_home"})

    is_image = str(content_type).startswith("image/") if content_type else False
    if not content_type:
        from .providers.documents import FileDocumentProvider

        ext = path.suffix.lower()
        detected = FileDocumentProvider.EXTENSION_TYPES.get(ext, "")
        is_image = detected.startswith("image/")
        if is_image:
            content_type = detected

    if is_image:
        full_content = keeper._ocr_image(path, content_type, extractor)
    else:
        full_content = keeper._ocr_pdf(path, ocr_pages, extractor)

    if not full_content or not full_content.strip():
        logger.info("OCR produced no usable text for %s", uri)
        return TaskRunResult(status="skipped", details={"reason": "no_usable_text"})

    existing = keeper._document_store.get(req.collection, req.id)
    if not existing:
        return TaskRunResult(status="skipped", details={"reason": "deleted"})

    context = None
    user_tags = filter_non_system_tags(existing.tags)
    if user_tags:
        context = keeper._gather_context(req.id, user_tags)

    try:
        result = process_ocr(
            full_content,
            max_summary_length=keeper._config.max_summary_length,
            context=context,
            summarization_provider=keeper._get_summarization_provider(),
        )
    except Exception as e:
        logger.warning("OCR summarization failed for %s: %s", uri, e)
        result = process_ocr(
            full_content,
            max_summary_length=keeper._config.max_summary_length,
            context=context,
            summarization_provider=None,
        )

    keeper.apply_result(req.id, req.collection, result, existing_tags=existing.tags)
    return TaskRunResult(
        status="applied",
        details={"chars": len(full_content), "content_type": content_type or "pdf"},
    )


def _run_tag(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    content = str(req.content or "").strip()
    if not content:
        return TaskRunResult(status="skipped", details={"reason": "no_content"})

    provider_name = str(req.metadata.get("provider") or "noop").strip()
    provider_params = req.metadata.get("provider_params")
    if provider_params is not None and not isinstance(provider_params, dict):
        provider_params = None

    from .providers.base import get_registry

    registry = get_registry()
    provider = registry.create_tagging(provider_name, provider_params)
    tag_method = getattr(provider, "tag", None)
    if not callable(tag_method):
        return TaskRunResult(status="skipped", details={"reason": "provider_has_no_tag"})

    tags = tag_method(content)
    if not isinstance(tags, dict):
        return TaskRunResult(status="skipped", details={"reason": "invalid_provider_output"})

    normalized = {str(k): str(v) for k, v in tags.items() if str(k).strip() and str(v).strip()}
    if not normalized:
        return TaskRunResult(status="skipped", details={"reason": "empty_tags"})

    keeper.tag(req.id, tags=normalized)
    return TaskRunResult(status="applied", details={"tag_count": len(normalized)})


def run_local_task(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    """Run one local task workflow."""
    task_type = str(req.task_type or "").strip()
    if task_type == "summarize":
        return _run_summarize(keeper, req)
    if task_type == "analyze":
        return _run_analyze(keeper, req)
    if task_type == "ocr":
        return _run_ocr(keeper, req)
    if task_type == "tag":
        return _run_tag(keeper, req)
    raise ValueError(f"unsupported local task workflow: {task_type}")
