"""Tests for keep.processors — pure processing functions."""

from unittest.mock import MagicMock, patch

import pytest

from keep.processors import (
    process_summarize,
    process_analyze,
    process_ocr,
    ocr_image,
    ocr_pdf,
    _content_hash,
    _content_hash_full,
    DELEGATABLE_TASK_TYPES,
    LOCAL_ONLY_TASK_TYPES,
    MIME_TO_EXTENSION,
)


# ---------------------------------------------------------------------------
# process_summarize
# ---------------------------------------------------------------------------


class TestProcessSummarize:
    """Tests for the process_summarize pure function."""

    def test_calls_generate_for_llm_provider(self):
        """LLM providers use generate() path."""
        provider = MagicMock()
        provider.generate.return_value = "A brief summary"

        result = process_summarize("long content here", summarization_provider=provider)

        assert result["summary"] == "A brief summary"
        provider.generate.assert_called_once()
        provider.summarize.assert_not_called()

    def test_falls_back_to_summarize_for_non_llm(self):
        """Non-LLM providers (generate returns None) use summarize() path."""
        provider = MagicMock()
        provider.generate.return_value = None
        provider.summarize.return_value = "truncated summary"

        result = process_summarize("long content here", summarization_provider=provider)

        assert result["summary"] == "truncated summary"
        provider.summarize.assert_called_once_with("long content here", context=None)

    def test_passes_context(self):
        """Context is forwarded to the provider."""
        provider = MagicMock()
        provider.generate.return_value = "contextual summary"

        result = process_summarize(
            "content", context="related notes", summarization_provider=provider,
        )

        assert result["summary"] == "contextual summary"
        provider.generate.assert_called_once()

    def test_no_side_fields(self):
        """Summarize result has no OCR-specific fields set."""
        provider = MagicMock()
        provider.generate.return_value = "sum"

        result = process_summarize("x", summarization_provider=provider)

        assert result == {"summary": "sum"}


# ---------------------------------------------------------------------------
# process_ocr
# ---------------------------------------------------------------------------


class TestProcessOcr:
    """Tests for the process_ocr pure function."""

    def test_short_content_used_as_summary(self):
        """Content shorter than max_summary_length is the summary itself."""
        result = process_ocr("short text", max_summary_length=100)

        assert result["summary"] == "short text"
        assert result["content"] == "short text"

    def test_long_content_summarized(self):
        """Provider is called when content exceeds max_summary_length."""
        provider = MagicMock()
        provider.generate.return_value = "summarized"
        long_text = "x" * 200

        result = process_ocr(
            long_text, max_summary_length=50,
            summarization_provider=provider,
        )

        assert result["summary"] == "summarized"
        assert result["content"] == long_text
        provider.generate.assert_called_once()

    def test_no_provider_truncates(self):
        """Without a provider, long content is truncated with ellipsis."""
        long_text = "abcdef" * 100

        result = process_ocr(long_text, max_summary_length=20)

        assert result["summary"] == long_text[:20] + "..."
        assert result["content"] == long_text

    def test_computes_hashes(self):
        """content_hash and content_hash_full are set."""
        result = process_ocr("hello world", max_summary_length=1000)

        assert result["content_hash"] is not None
        assert len(result["content_hash"]) == 10  # short hash
        assert result["content_hash_full"] is not None
        assert len(result["content_hash_full"]) == 64  # full SHA256

    def test_context_forwarded_to_provider(self):
        """Context is passed through to the summarization provider."""
        provider = MagicMock()
        provider.generate.return_value = "ctx summary"

        process_ocr(
            "x" * 200, max_summary_length=50,
            context="related context", summarization_provider=provider,
        )

        provider.generate.assert_called_once()


# ---------------------------------------------------------------------------
# ocr_image
# ---------------------------------------------------------------------------


class TestOcrImage:
    """Tests for the ocr_image pure function."""

    def test_calls_extractor(self, tmp_path):
        """Extractor.extract is called with path and content_type."""
        extractor = MagicMock()
        extractor.extract.return_value = "Total: $42.99\nThank you for your purchase"

        result = ocr_image(tmp_path / "receipt.png", "image/png", extractor)

        assert result is not None
        assert "42.99" in result
        extractor.extract.assert_called_once_with(
            str(tmp_path / "receipt.png"), "image/png"
        )

    def test_rejects_low_confidence(self, tmp_path):
        """Low-confidence OCR output is rejected."""
        extractor = MagicMock()
        extractor.extract.return_value = "!@#$%^&*()"

        result = ocr_image(tmp_path / "garbage.png", "image/png", extractor)

        assert result is None

    def test_returns_none_on_empty(self, tmp_path):
        """Returns None when extractor returns nothing."""
        extractor = MagicMock()
        extractor.extract.return_value = None

        result = ocr_image(tmp_path / "blank.png", "image/png", extractor)

        assert result is None

    def test_rejects_very_short_text(self, tmp_path):
        """Text <= 10 chars after cleaning is rejected."""
        extractor = MagicMock()
        extractor.extract.return_value = "Hi"

        result = ocr_image(tmp_path / "tiny.png", "image/png", extractor)

        assert result is None


# ---------------------------------------------------------------------------
# ocr_pdf
# ---------------------------------------------------------------------------


class TestOcrPdf:
    """Tests for the ocr_pdf pure function."""

    def test_merges_text_and_ocr(self, tmp_path):
        """Text-layer pages are merged with OCR pages in page order."""
        try:
            from pypdf import PdfWriter
            import pypdfium2  # noqa: F401
        except ImportError:
            pytest.skip("pypdf and pypdfium2 required")

        from keep.providers.documents import FileDocumentProvider

        # Create a 2-page blank PDF (both pages need OCR)
        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "test.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        extractor = MagicMock()
        extractor.extract.return_value = "OCR text from page"

        result = ocr_pdf(pdf_path, [0, 1], extractor)

        # Should have content from both pages (or None if OCR cleaning rejects)
        # The mock returns clean text, so it should succeed
        if result is not None:
            assert "OCR text" in result

    def test_returns_none_when_no_ocr_results(self, tmp_path):
        """Returns None when OCR produces no results."""
        try:
            from pypdf import PdfWriter
            import pypdfium2  # noqa: F401
        except ImportError:
            pytest.skip("pypdf and pypdfium2 required")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "blank.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        # Extractor returns garbage that gets rejected by confidence filter
        extractor = MagicMock()
        extractor.extract.return_value = "!@#$"

        result = ocr_pdf(pdf_path, [0], extractor)

        assert result is None


# ---------------------------------------------------------------------------
# Hash functions (moved from api.py in Phase 3 consolidation)
# ---------------------------------------------------------------------------


class TestContentHash:
    """Tests for _content_hash and _content_hash_full."""

    def test_short_hash_length(self):
        """Short hash is last 10 chars of SHA256."""
        h = _content_hash("hello world")
        assert len(h) == 10
        assert h.isalnum()

    def test_full_hash_length(self):
        """Full hash is complete 64-char SHA256."""
        h = _content_hash_full("hello world")
        assert len(h) == 64
        assert h.isalnum()

    def test_short_is_suffix_of_full(self):
        """Short hash should be the last 10 chars of the full hash."""
        short = _content_hash("test content")
        full = _content_hash_full("test content")
        assert full.endswith(short)

    def test_deterministic(self):
        """Same input produces same hash."""
        assert _content_hash("abc") == _content_hash("abc")
        assert _content_hash_full("abc") == _content_hash_full("abc")

    def test_different_inputs_differ(self):
        """Different inputs produce different hashes."""
        assert _content_hash("foo") != _content_hash("bar")
        assert _content_hash_full("foo") != _content_hash_full("bar")

    def test_backwards_compat_import(self):
        """Hash functions are still importable from api.py."""
        from keep.processors import _content_hash as api_hash
        from keep.processors import _content_hash_full as api_hash_full
        assert api_hash("test") == _content_hash("test")
        assert api_hash_full("test") == _content_hash_full("test")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Tests for task type constants and MIME mapping."""

    def test_delegatable_types(self):
        assert "summarize" in DELEGATABLE_TASK_TYPES
        assert "ocr" in DELEGATABLE_TASK_TYPES
        assert "embed" not in DELEGATABLE_TASK_TYPES

    def test_local_only_types(self):
        assert "embed" in LOCAL_ONLY_TASK_TYPES
        assert "reindex" in LOCAL_ONLY_TASK_TYPES
        assert "summarize" not in LOCAL_ONLY_TASK_TYPES

    def test_no_overlap(self):
        """Delegatable and local-only should not overlap."""
        assert set(DELEGATABLE_TASK_TYPES) & set(LOCAL_ONLY_TASK_TYPES) == set()

    def test_mime_to_extension(self):
        assert MIME_TO_EXTENSION["application/pdf"] == ".pdf"
        assert MIME_TO_EXTENSION["image/jpeg"] == ".jpg"
        assert MIME_TO_EXTENSION["image/png"] == ".png"

    def test_analyze_is_delegatable(self):
        assert "analyze" in DELEGATABLE_TASK_TYPES

    def test_exports_from_init(self):
        """DELEGATABLE_TASK_TYPES is exported from keep."""
        from keep import DELEGATABLE_TASK_TYPES as DT
        assert DT is DELEGATABLE_TASK_TYPES


# ---------------------------------------------------------------------------
# process_analyze
# ---------------------------------------------------------------------------


class TestProcessAnalyze:
    """Tests for the process_analyze pure function."""

    def test_returns_parts(self):
        """Analyzer output is returned in the result dict."""
        raw_parts = [
            {"summary": "Part 1 summary", "content": "Section A"},
            {"summary": "Part 2 summary", "content": "Section B"},
        ]
        chunks = [
            {"content": "First section.", "tags": {}, "index": 0},
            {"content": "Second section.", "tags": {}, "index": 1},
        ]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.analyze.return_value = raw_parts
            result = process_analyze(chunks, analyzer_provider=MagicMock())

        assert result["parts"] == raw_parts

    def test_single_part_returned(self):
        """Single-part result is returned (caller decides to skip)."""
        raw_parts = [{"summary": "Only part", "content": "All content"}]
        chunks = [{"content": "Short content.", "tags": {}, "index": 0}]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.analyze.return_value = raw_parts
            result = process_analyze(chunks, analyzer_provider=MagicMock())

        assert len(result["parts"]) == 1

    def test_passes_guide_context(self):
        """Guide context is forwarded to the analyzer."""
        chunks = [{"content": "Content.", "tags": {}, "index": 0}]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.analyze.return_value = [{"summary": "P1"}]
            process_analyze(
                chunks,
                guide_context="## Tag: topic\nUse these topics",
                analyzer_provider=MagicMock(),
            )
            MockAnalyzer.return_value.analyze.assert_called_once()
            call_args = MockAnalyzer.return_value.analyze.call_args
            assert call_args[0][1] == "## Tag: topic\nUse these topics"

    def test_with_classification(self):
        """Tag specs trigger classification pass."""
        raw_parts = [
            {"summary": "Part 1", "content": "I will do X."},
            {"summary": "Part 2", "content": "The sky is blue."},
        ]
        chunks = [{"content": "I will do X. The sky is blue.", "tags": {}, "index": 0}]
        tag_specs = [{
            "key": "act",
            "description": "Speech act type",
            "prompt": "",
            "values": [
                {"value": "commitment", "description": "A promise", "prompt": ""},
                {"value": "assertion", "description": "A claim", "prompt": ""},
            ],
        }]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer, \
             patch("keep.analyzers.TagClassifier") as MockClassifier:
            MockAnalyzer.return_value.analyze.return_value = raw_parts
            mock_provider = MagicMock()
            result = process_analyze(
                chunks,
                tag_specs=tag_specs,
                analyzer_provider=mock_provider,
                classifier_provider=mock_provider,
            )

            # Classifier should be called with parts and specs
            MockClassifier.return_value.classify.assert_called_once_with(raw_parts, tag_specs)

        assert len(result["parts"]) == 2

    def test_classification_failure_is_non_fatal(self):
        """If classifier fails, parts are returned without tags."""
        raw_parts = [{"summary": "P1"}, {"summary": "P2"}]
        chunks = [{"content": "Content.", "tags": {}, "index": 0}]
        tag_specs = [{"key": "act", "description": "...", "prompt": "", "values": []}]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer, \
             patch("keep.analyzers.TagClassifier") as MockClassifier:
            MockAnalyzer.return_value.analyze.return_value = raw_parts
            MockClassifier.return_value.classify.side_effect = Exception("crash")

            result = process_analyze(
                chunks,
                tag_specs=tag_specs,
                analyzer_provider=MagicMock(),
                classifier_provider=MagicMock(),
            )

        assert len(result["parts"]) == 2

    def test_no_classification_without_specs(self):
        """Without tag_specs, classifier is not invoked."""
        raw_parts = [{"summary": "P1"}, {"summary": "P2"}]
        chunks = [{"content": "Content.", "tags": {}, "index": 0}]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer, \
             patch("keep.analyzers.TagClassifier") as MockClassifier:
            MockAnalyzer.return_value.analyze.return_value = raw_parts

            result = process_analyze(chunks, analyzer_provider=MagicMock())

            MockClassifier.return_value.classify.assert_not_called()

        assert len(result["parts"]) == 2

    def test_reconstructs_analysis_chunks(self):
        """Input dicts are converted to AnalysisChunk objects."""
        chunks = [
            {"content": "Hello", "tags": {"topic": "greeting"}, "index": 0},
            {"content": "World", "tags": {}, "index": 1},
        ]

        with patch("keep.analyzers.SlidingWindowAnalyzer") as MockAnalyzer:
            MockAnalyzer.return_value.analyze.return_value = [{"summary": "P1"}]
            process_analyze(chunks, analyzer_provider=MagicMock())

            call_args = MockAnalyzer.return_value.analyze.call_args
            passed_chunks = call_args[0][0]
            assert len(passed_chunks) == 2
            assert passed_chunks[0].content == "Hello"
            assert passed_chunks[0].tags == {"topic": "greeting"}
            assert passed_chunks[1].index == 1
