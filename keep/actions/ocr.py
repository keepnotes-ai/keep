from __future__ import annotations

"""Item-scoped OCR extraction action."""

import logging
from pathlib import Path
from typing import Any

from ..paths import validate_path_within_home
from ..processors import ocr_image, ocr_pdf
from . import action
from ._item_scope import resolve_item

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
        """Run OCR and return extracted text with mutations."""
        import hashlib

        item_id, item = resolve_item(params, context)

        tags = getattr(item, "tags", None)
        tags = dict(tags) if isinstance(tags, dict) else {}
        uri = str(
            params.get("uri")
            or getattr(item, "uri", "")
            or tags.get("_source_uri")
            or item_id
        ).strip()
        if not uri:
            raise ValueError("ocr requires uri source")

        content_type = str(
            params.get("content_type") or tags.get("_content_type") or ""
        ).strip()
        pages = _coerce_pages(params.get("ocr_pages") or params.get("pages"))
        if not pages:
            pages = _coerce_pages(tags.get("_ocr_pages"))

        path = Path(uri.removeprefix("file://")).resolve()
        if not path.exists():
            logger.warning("File no longer exists for OCR: %s", path)
            return {"text": "", "skipped": True, "reason": "missing_file"}

        try:
            validate_path_within_home(path)
        except ValueError:
            logger.warning("OCR path outside home directory, skipping: %s", path)
            return {"text": "", "skipped": True, "reason": "path_outside_home"}

        extractor = context.resolve_provider("content_extractor")
        if extractor is None:
            raise ValueError("ocr requires configured content extractor provider")

        # Detect content type if not provided
        is_image = content_type.startswith("image/") if content_type else False
        if not content_type:
            from ..providers.documents import FileDocumentProvider
            ext = path.suffix.lower()
            detected = FileDocumentProvider.EXTENSION_TYPES.get(ext, "")
            is_image = detected.startswith("image/")
            if is_image:
                content_type = detected

        if not pages and not is_image:
            return {"text": "", "pages_processed": 0, "skipped": True, "reason": "no_ocr_pages"}

        # Extract text
        if is_image:
            text = ocr_image(path, content_type, extractor)
            pages_processed = 1 if text else 0
        else:
            max_ocr_pages = 1000
            if len(pages) > max_ocr_pages:
                logger.warning(
                    "OCR page count %d exceeds limit %d for %s — truncating",
                    len(pages), max_ocr_pages, uri,
                )
                pages = pages[:max_ocr_pages]
            text = ocr_pdf(path, pages, extractor)
            pages_processed = len(pages)

        extracted = "" if text is None else str(text)
        if not extracted.strip():
            logger.info("OCR produced no usable text for %s", uri)
            return {"text": "", "pages_processed": pages_processed, "skipped": True, "reason": "no_usable_text"}

        # Summarize if text is long
        max_summary_length = int(params.get("max_summary_length", 2000))
        if len(extracted) <= max_summary_length:
            summary = extracted
        else:
            try:
                provider = context.resolve_provider("summarization")
                summarized = provider.summarize(extracted)
                summary = str(summarized) if summarized else extracted[:max_summary_length] + "..."
            except Exception:
                summary = extracted[:max_summary_length] + "..."

        content_hash = hashlib.sha256(extracted.encode("utf-8")).hexdigest()[-10:]
        content_hash_full = hashlib.sha256(extracted.encode("utf-8")).hexdigest()

        return {
            "text": extracted,
            "summary": summary,
            "pages_processed": pages_processed,
            "mutations": [
                {
                    "op": "set_content",
                    "target": item_id,
                    "content": extracted,
                    "summary": summary,
                    "content_hash": content_hash,
                    "content_hash_full": content_hash_full,
                }
            ],
        }
