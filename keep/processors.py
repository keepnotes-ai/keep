"""Pure processing functions for keep.

These functions encapsulate the "compute" portion of background processing
(summarization, OCR) without any store reads or writes.  This separation
allows the same processing logic to run locally or be delegated to a
hosted service (Phase 1 of Hybrid Processing).

Each function returns a ProcessorResult that the caller applies to the store
via Keeper.apply_result().
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


# --- Hash functions (used by OCR processing and document dedup) ---

def _content_hash(content: str) -> str:
    """Short SHA256 hash of content for change detection."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[-10:]


def _content_hash_full(content: str) -> str:
    """Full SHA256 hash of content for dedup verification."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


# --- Task type constants ---

# Task types that can be delegated to the hosted service
DELEGATABLE_TASK_TYPES = ("summarize", "ocr", "analyze")

# Task types that must run locally (need local store access)
LOCAL_ONLY_TASK_TYPES = ("embed", "reindex")

# MIME type → file extension for OCR temp files
MIME_TO_EXTENSION = {
    "application/pdf": ".pdf",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/tiff": ".tiff",
    "image/webp": ".webp",
}


@dataclass
class ProcessorResult:
    """Result of processing a task.  Caller applies to store."""

    task_type: str  # "summarize" | "ocr" | "analyze"
    summary: str | None = None
    content: str | None = None            # ocr: full extracted text
    content_hash: str | None = None       # ocr: short hash
    content_hash_full: str | None = None  # ocr: full SHA256
    parts: list | None = None             # analyze: PartInfo list (Phase 2)


def _llm_summarize(
    content: str,
    provider,
    *,
    context: str | None = None,
    system_prompt_override: str | None = None,
) -> str | None:
    """Build prompts and call provider.generate().  Returns None for non-LLM providers."""
    from .providers.base import (
        build_summarization_prompt,
        SUMMARIZATION_SYSTEM_PROMPT,
        strip_summary_preamble,
    )

    gen = getattr(provider, "generate", None)
    if gen is None:
        return None

    truncated = content[:50000] if len(content) > 50000 else content
    user_prompt = build_summarization_prompt(truncated, context)
    system = system_prompt_override or SUMMARIZATION_SYSTEM_PROMPT
    if context and not system_prompt_override:
        system = (
            "You are a helpful assistant that summarizes content. "
            "Follow the instructions in the user message."
        )
    result = gen(system, user_prompt)
    if result is None:
        return None
    return strip_summary_preamble(result)


def process_summarize(
    content: str,
    *,
    context: str | None = None,
    summarization_provider,
    system_prompt_override: str | None = None,
) -> ProcessorResult:
    """Summarize content.  Pure function — no store access."""
    summary = _llm_summarize(
        content, summarization_provider,
        context=context, system_prompt_override=system_prompt_override,
    )
    if summary is None:
        # Non-LLM fallback (truncate, first_paragraph)
        summary = summarization_provider.summarize(content, context=context)
    return ProcessorResult(task_type="summarize", summary=summary)


def ocr_image(path: Path, content_type: str, extractor) -> str | None:
    """OCR a single image file.  Returns cleaned text or None."""
    from .providers.documents import FileDocumentProvider

    text = extractor.extract(str(path), content_type)
    if not text:
        return None
    cleaned = FileDocumentProvider._clean_ocr_text(text)
    confidence = FileDocumentProvider._estimate_ocr_confidence(cleaned)
    if confidence < 0.3 or len(cleaned) <= 10:
        logger.info("Image OCR low confidence (%.2f) for %s", confidence, path.name)
        return None
    return cleaned


def ocr_pdf(path: Path, ocr_pages: list[int], extractor) -> str | None:
    """OCR scanned PDF pages and merge with text-layer pages.  Returns text or None."""
    from .providers.documents import FileDocumentProvider

    file_provider = FileDocumentProvider()
    ocr_results = file_provider._ocr_pdf_pages(path, ocr_pages, extractor=extractor)

    if not ocr_results:
        return None

    # Re-extract text pages (fast) and merge with OCR
    from pypdf import PdfReader

    reader = PdfReader(str(path))
    text_parts: list[tuple[int, str]] = []
    ocr_set = set(ocr_pages)
    for i, page in enumerate(reader.pages):
        if i not in ocr_set:
            text = page.extract_text()
            if text and text.strip():
                text_parts.append((i, text))
    text_parts.extend(ocr_results)
    text_parts.sort(key=lambda t: t[0])
    return "\n\n".join(text for _, text in text_parts)


def process_ocr(
    full_content: str,
    *,
    max_summary_length: int,
    context: str | None = None,
    summarization_provider=None,
) -> ProcessorResult:
    """Process OCR'd text: summarize if needed, compute hashes.

    Pure function — no store access.
    """
    if len(full_content) <= max_summary_length:
        summary = full_content
    elif summarization_provider:
        summary = _llm_summarize(full_content, summarization_provider, context=context)
        if summary is None:
            summary = summarization_provider.summarize(full_content, context=context)
    else:
        summary = full_content[:max_summary_length] + "..."

    return ProcessorResult(
        task_type="ocr",
        summary=summary,
        content=full_content,
        content_hash=_content_hash(full_content),
        content_hash_full=_content_hash_full(full_content),
    )


def process_analyze(
    chunks: list[dict],
    guide_context: str = "",
    tag_specs: list[dict] | None = None,
    *,
    analyzer_provider=None,
    classifier_provider=None,
    prompt_override: str | None = None,
) -> ProcessorResult:
    """Analyze + classify content into parts.  Pure function — no store access.

    Args:
        chunks: Serializable chunk dicts [{"content": str, "tags": dict, "index": int}].
        guide_context: Tag guidance descriptions for the analyzer.
        tag_specs: Tag taxonomy specs for classification (from TagClassifier.load_specs).
        analyzer_provider: SummarizationProvider for the analyzer LLM.
        classifier_provider: SummarizationProvider for the classifier LLM
            (defaults to analyzer_provider if not set).
        prompt_override: Analysis prompt text from .prompt/analyze/* docs.

    Returns:
        ProcessorResult with parts=[{"summary": str, "content": str, "tags": dict}, ...]
    """
    from .analyzers import SlidingWindowAnalyzer, TagClassifier
    from .providers.base import AnalysisChunk

    # Reconstruct AnalysisChunk objects from serializable dicts
    analysis_chunks = [
        AnalysisChunk(
            content=c.get("content", ""),
            tags=c.get("tags", {}),
            index=c.get("index", i),
        )
        for i, c in enumerate(chunks)
    ]

    # Run analyzer
    analyzer = SlidingWindowAnalyzer(provider=analyzer_provider)
    raw_parts = analyzer.analyze(
        analysis_chunks, guide_context, prompt_override=prompt_override,
    )

    # Run classifier if tag specs provided
    if tag_specs and raw_parts:
        try:
            classifier = TagClassifier(
                provider=classifier_provider or analyzer_provider,
            )
            classifier.classify(raw_parts, tag_specs)
        except Exception as e:
            logger.warning("Tag classification skipped: %s", e)

    return ProcessorResult(task_type="analyze", parts=raw_parts)
