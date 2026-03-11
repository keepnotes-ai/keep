from __future__ import annotations

"""Item-scoped OCR extraction action."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..paths import validate_path_within_home
from ..processors import ocr_image, ocr_pdf, process_ocr
from ..types import filter_non_system_tags
from . import action
from ._item_scope import resolve_item

if TYPE_CHECKING:
    from ..api import Keeper
    from ..task_workflows import TaskRequest, TaskRunResult

logger = logging.getLogger(__name__)


def _coerce_pages(value: Any) -> list[int]:
    """Coerce user-provided page selectors into integer page numbers."""
    if isinstance(value, list):
        pages: list[int] = []
        for part in value:
            try:
                pages.append(int(part))
            except Exception:
                continue
        return pages
    return []


@action(id="ocr")
class Ocr:
    """Extract OCR text for URI-backed target items."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Run OCR and return extracted text with an upsert mutation."""
        item_id, item = resolve_item(params, context)

        tags = getattr(item, "tags", None)
        tags = dict(tags) if isinstance(tags, dict) else {}
        uri = str(getattr(item, "uri", "") or tags.get("_source_uri") or item_id).strip()
        if not uri:
            raise ValueError("ocr requires uri source")
        path = validate_path_within_home(Path(uri.removeprefix("file://")))
        if not path.exists():
            raise ValueError(f"ocr source does not exist: {path}")

        content_type = str(tags.get("_content_type") or "").strip()
        pages = _coerce_pages(params.get("pages"))
        if not pages:
            pages = _coerce_pages(tags.get("_ocr_pages"))

        extractor = context.resolve_provider("content_extractor")
        if extractor is None:
            raise ValueError("ocr requires configured content extractor provider")

        is_image = content_type.startswith("image/")
        if is_image:
            text = ocr_image(path, content_type, extractor)
            pages_processed = 1 if text else 0
        else:
            if not pages:
                return {"text": "", "pages_processed": 0}
            text = ocr_pdf(path, pages, extractor)
            pages_processed = len(pages)

        extracted = "" if text is None else str(text)
        out: dict[str, Any] = {
            "text": extracted,
            "pages_processed": pages_processed,
        }
        if extracted.strip():
            out["mutations"] = [
                {
                    "op": "upsert_item",
                    "target": item_id,
                    "content": "$output.text",
                }
            ]
        return out

    def run_task(self, keeper: "Keeper", req: "TaskRequest") -> "TaskRunResult":
        """Background task workflow for OCR extraction."""
        from ..task_workflows import TaskRunResult

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

        try:
            validate_path_within_home(path)
        except ValueError:
            logger.warning("OCR path outside home directory, skipping: %s", path)
            return TaskRunResult(status="skipped", details={"reason": "path_outside_home"})

        is_image = str(content_type).startswith("image/") if content_type else False
        if not content_type:
            from ..providers.documents import FileDocumentProvider

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
