from __future__ import annotations

"""Item-scoped OCR extraction action."""

from pathlib import Path
from typing import Any

from ..paths import validate_path_within_home
from ..processors import ocr_image, ocr_pdf
from . import action
from ._item_scope import resolve_item


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
