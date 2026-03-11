"""Tests for OCR content extraction pipeline."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# FileDocumentProvider: OCR text cleaning and confidence
# ---------------------------------------------------------------------------


class TestCleanOcrText:
    """Test OCR text cleaning logic."""

    def _clean(self, text):
        from keep.providers.documents import FileDocumentProvider
        return FileDocumentProvider._clean_ocr_text(text)

    def test_strips_short_lines(self):
        assert self._clean("a\nHello World\nb") == "Hello World"

    def test_strips_no_space_blobs(self):
        # Lines >20 chars with no spaces are likely garbage
        assert self._clean("abcdefghijklmnopqrstuvwxyz") == ""

    def test_strips_non_alphanumeric_lines(self):
        assert self._clean("---\n===\nHello") == "Hello"

    def test_preserves_good_lines(self):
        text = "Chapter 1\nThis is real content.\nPage 42"
        cleaned = self._clean(text)
        assert "Chapter 1" in cleaned
        assert "This is real content." in cleaned
        assert "Page 42" in cleaned

    def test_empty_input(self):
        assert self._clean("") == ""

    def test_all_junk(self):
        assert self._clean("a\n.\n---") == ""


class TestEstimateOcrConfidence:
    """Test OCR confidence estimation."""

    def _conf(self, text):
        from keep.providers.documents import FileDocumentProvider
        return FileDocumentProvider._estimate_ocr_confidence(text)

    def test_empty(self):
        assert self._conf("") == 0.0

    def test_good_text(self):
        # "Hello World" has 10 alnum out of 11 total
        conf = self._conf("Hello World")
        assert conf > 0.8

    def test_garbage(self):
        # All symbols
        conf = self._conf("!@#$%^&*()")
        assert conf == 0.0

    def test_mixed(self):
        # 50/50 mix
        conf = self._conf("ab!!")
        assert 0.4 <= conf <= 0.6


# ---------------------------------------------------------------------------
# FileDocumentProvider: PDF text extraction returns (text, ocr_pages) tuple
# ---------------------------------------------------------------------------


class TestPdfTextExtraction:
    """Test _extract_pdf_text returns text + OCR page indices."""

    def test_blank_pdf_returns_empty_with_ocr_pages(self, tmp_path):
        """Blank PDF returns empty text + OCR page indices."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()
        try:
            from pypdf import PdfWriter
        except ImportError:
            pytest.skip("pypdf not available")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "blank.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        # Blank PDF: empty text, page 0 needs OCR
        text, ocr_pages = provider._extract_pdf_text(pdf_path)
        assert text == ""
        assert ocr_pages == [0]

    def test_blank_pdf_returns_ocr_pages(self, tmp_path):
        """Blank PDF returns empty text + page indices needing OCR."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()

        try:
            from pypdf import PdfWriter
        except ImportError:
            pytest.skip("pypdf not available")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "scanned.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        text, ocr_pages = provider._extract_pdf_text(pdf_path)
        assert text == ""
        assert ocr_pages == [0, 1]

    def test_extractor_none_still_reports_ocr_pages(self, tmp_path):
        """Without an extractor, blank pages are still reported for OCR."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()

        try:
            from pypdf import PdfWriter
        except ImportError:
            pytest.skip("pypdf not available")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "noop.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        # Returns empty text + OCR pages (caller decides if OCR is available)
        text, ocr_pages = provider._extract_pdf_text(pdf_path)
        assert text == ""
        assert ocr_pages == [0]

    def test_fetch_includes_ocr_pages_in_metadata(self, tmp_path, monkeypatch):
        """fetch() passes _ocr_pages through Document metadata."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()

        # tmp_path may be outside home dir; patch home to allow it
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        try:
            from pypdf import PdfWriter
        except ImportError:
            pytest.skip("pypdf not available")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "scanned.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        doc = provider.fetch(str(pdf_path))
        assert doc.metadata is not None
        assert "_ocr_pages" in doc.metadata
        assert doc.metadata["_ocr_pages"] == [0]
        assert "pending OCR" in doc.content

    def test_fetch_no_ocr_pages_for_text_pdf(self, tmp_path):
        """Text-layer PDF has no _ocr_pages in metadata."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()

        try:
            from pypdf import PdfWriter
            from pypdf.generic import NameObject, ArrayObject, DictionaryObject
        except ImportError:
            pytest.skip("pypdf not available")

        # Create a PDF with actual text content using reportlab or raw PDF ops
        # For simplicity, use a blank page (which has no text) — but this
        # actually tests the blank path. For a proper text PDF test we'd need
        # reportlab. Skip this specific assertion if we can't create text PDFs.
        pytest.skip("Creating PDFs with text layers requires reportlab")


# ---------------------------------------------------------------------------
# _ocr_pdf_pages: background OCR rendering and extraction
# ---------------------------------------------------------------------------


class TestOcrPdfPages:
    """Test _ocr_pdf_pages (used by background processor)."""

    def test_ocr_called_for_blank_pages(self, tmp_path):
        """_ocr_pdf_pages calls extractor for each page."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()

        # Mock extractor that returns text
        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = "OCR extracted text from page"

        try:
            from pypdf import PdfWriter
        except ImportError:
            pytest.skip("pypdf not available")

        try:
            import pypdfium2  # noqa: F401
        except ImportError:
            pytest.skip("pypdfium2 not available")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "scanned.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        results = provider._ocr_pdf_pages(pdf_path, [0], extractor=mock_extractor)
        assert len(results) == 1
        assert results[0][0] == 0  # page index
        assert "OCR extracted text from page" in results[0][1]
        mock_extractor.extract.assert_called_once()
        # Called with a .png path and "image/png"
        call_args = mock_extractor.extract.call_args
        assert call_args[0][1] == "image/png"
        assert call_args[0][0].endswith(".png")

    def test_low_confidence_rejected(self, tmp_path):
        """OCR output with low confidence is filtered out."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()

        # Mock extractor that returns garbage
        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = "!@#$%^&*()"

        try:
            from pypdf import PdfWriter
            import pypdfium2  # noqa: F401
        except ImportError:
            pytest.skip("pypdf and pypdfium2 required")

        writer = PdfWriter()
        writer.add_blank_page(width=200, height=200)
        pdf_path = tmp_path / "garbage.pdf"
        with open(pdf_path, "wb") as f:
            writer.write(f)

        results = provider._ocr_pdf_pages(pdf_path, [0], extractor=mock_extractor)
        # Garbage OCR gets rejected
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Image OCR
# ---------------------------------------------------------------------------


class TestImageOcr:
    """Test OCR for image files (PNG, JPG, etc.)."""

    def test_fetch_sets_ocr_pages_for_image(self, tmp_path, monkeypatch):
        """fetch() signals OCR needed for image files."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        # Create a minimal PNG (1x1 pixel)
        from PIL import Image
        img = Image.new("RGB", (100, 100), color="white")
        img_path = tmp_path / "receipt.png"
        img.save(img_path)

        doc = provider.fetch(f"file://{img_path}")
        assert doc.metadata.get("_ocr_pages") == [0]
        assert doc.content_type == "image/png"

    def test_fetch_sets_ocr_pages_for_jpeg(self, tmp_path, monkeypatch):
        """fetch() signals OCR for JPEG images."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        from PIL import Image
        img = Image.new("RGB", (100, 100), color="white")
        img_path = tmp_path / "photo.jpg"
        img.save(img_path)

        doc = provider.fetch(f"file://{img_path}")
        assert doc.metadata.get("_ocr_pages") == [0]
        assert doc.content_type == "image/jpeg"

    def test_image_metadata_still_in_content(self, tmp_path, monkeypatch):
        """Image EXIF metadata is still extracted as content."""
        from keep.providers.documents import FileDocumentProvider

        provider = FileDocumentProvider()
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))

        from PIL import Image
        img = Image.new("RGB", (200, 300), color="white")
        img_path = tmp_path / "test.png"
        img.save(img_path)

        doc = provider.fetch(f"file://{img_path}")
        assert "200x300" in doc.content

    def test_ocr_image_calls_extractor(self, tmp_path, mock_providers):
        """_ocr_image calls the extractor and cleans the result."""
        from keep.api import Keeper

        kp = Keeper(str(tmp_path / "store"))

        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = "Total: $42.99\nThank you for your purchase"

        result = kp._ocr_image(tmp_path / "receipt.png", "image/png", mock_extractor)
        assert result is not None
        assert "42.99" in result
        mock_extractor.extract.assert_called_once_with(
            str(tmp_path / "receipt.png"), "image/png"
        )
        kp.close()

    def test_ocr_image_rejects_garbage(self, tmp_path, mock_providers):
        """_ocr_image rejects low-confidence OCR output."""
        from keep.api import Keeper

        kp = Keeper(str(tmp_path / "store"))

        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = "!@#$%^&*()"

        result = kp._ocr_image(tmp_path / "garbage.png", "image/png", mock_extractor)
        assert result is None
        kp.close()

    def test_ocr_image_returns_none_on_empty(self, tmp_path, mock_providers):
        """_ocr_image returns None when extractor returns nothing."""
        from keep.api import Keeper

        kp = Keeper(str(tmp_path / "store"))

        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = None

        result = kp._ocr_image(tmp_path / "blank.png", "image/png", mock_extractor)
        assert result is None
        kp.close()

    def test_ocr_workflow_dispatches_image(self, tmp_path, mock_providers):
        """Ocr.run_task routes to _ocr_image for image content types."""
        from keep.api import Keeper
        from keep.task_workflows import TaskRequest
        from keep.actions.ocr import Ocr

        kp = Keeper(str(tmp_path / "store"))

        # Create a test image
        from PIL import Image
        img = Image.new("RGB", (100, 100), color="white")
        img_path = tmp_path / "receipt.png"
        img.save(img_path)

        # Store a document so the workflow can find it
        kp.put("placeholder", id=f"file://{img_path}")

        kp._content_extractor = MagicMock()
        kp._ocr_image = MagicMock(return_value=None)

        req = TaskRequest(
            task_type="ocr",
            id=f"file://{img_path}",
            collection=kp._resolve_doc_collection(),
            content="",
            metadata={
                "uri": f"file://{img_path}",
                "ocr_pages": [0],
                "content_type": "image/png",
            },
        )

        with patch("keep.actions.ocr.validate_path_within_home"):
            result = Ocr().run_task(kp, req)

        kp._ocr_image.assert_called_once()
        call_args = kp._ocr_image.call_args[0]
        assert call_args[1] == "image/png"
        kp.close()

    def test_ocr_workflow_dispatches_pdf(self, tmp_path, mock_providers):
        """Ocr.run_task routes to _ocr_pdf for PDFs."""
        from keep.api import Keeper
        from keep.task_workflows import TaskRequest
        from keep.actions.ocr import Ocr

        kp = Keeper(str(tmp_path / "store"))

        pdf_path = tmp_path / "doc.pdf"
        pdf_path.write_bytes(b"%PDF-1.4")

        kp.put("placeholder", id=f"file://{pdf_path}")

        kp._content_extractor = MagicMock()
        kp._ocr_pdf = MagicMock(return_value=None)

        req = TaskRequest(
            task_type="ocr",
            id=f"file://{pdf_path}",
            collection=kp._resolve_doc_collection(),
            content="",
            metadata={
                "uri": f"file://{pdf_path}",
                "ocr_pages": [0, 1],
                "content_type": "application/pdf",
            },
        )

        with patch("keep.actions.ocr.validate_path_within_home"):
            result = Ocr().run_task(kp, req)

        kp._ocr_pdf.assert_called_once()
        kp.close()


# ---------------------------------------------------------------------------
# ContentExtractor protocol + registry
# ---------------------------------------------------------------------------


class TestContentExtractorRegistry:
    """Test content extractor registration and creation."""

    def test_protocol_defined(self):
        from keep.providers.base import ContentExtractor
        assert hasattr(ContentExtractor, 'extract')

    def test_register_and_create(self):
        from keep.providers.base import ProviderRegistry

        registry = ProviderRegistry()

        class MockExtractor:
            def __init__(self, model="test"):
                self.model = model

            def extract(self, path, content_type):
                return "extracted"

        registry.register_content_extractor("mock", MockExtractor)
        extractor = registry.create_content_extractor("mock", {"model": "test"})
        assert extractor.extract("/tmp/test.png", "image/png") == "extracted"

    def test_list_providers(self):
        from keep.providers.base import ProviderRegistry

        registry = ProviderRegistry()

        class MockExtractor:
            def extract(self, path, content_type):
                return None

        registry.register_content_extractor("mock", MockExtractor)
        assert "mock" in registry.list_content_extractor_providers()


# ---------------------------------------------------------------------------
# LockedContentExtractor
# ---------------------------------------------------------------------------


class TestLockedContentExtractor:
    """Test the locked wrapper for content extractors."""

    def test_delegates_extract(self):
        from keep.model_lock import LockedContentExtractor

        inner = MagicMock()
        inner.extract.return_value = "text from image"

        locked = LockedContentExtractor(inner, Path(tempfile.mktemp()))
        result = locked.extract("/tmp/test.png", "image/png")
        assert result == "text from image"
        inner.extract.assert_called_once_with("/tmp/test.png", "image/png")

    def test_release(self):
        from keep.model_lock import LockedContentExtractor

        inner = MagicMock()
        locked = LockedContentExtractor(inner, Path(tempfile.mktemp()))
        locked.release()
        assert locked._provider is None


# ---------------------------------------------------------------------------
# Config: content_extractor field
# ---------------------------------------------------------------------------


class TestContentExtractorConfig:
    """Test config loading/saving of content_extractor."""

    def test_storeconfig_has_field(self):
        from keep.config import StoreConfig
        config = StoreConfig(path=Path("/tmp/test"))
        assert config.content_extractor is None

    def test_save_and_load_roundtrip(self, tmp_path):
        from keep.config import StoreConfig, ProviderConfig, save_config, load_config

        config = StoreConfig(
            path=tmp_path,
            config_dir=tmp_path,
            content_extractor=ProviderConfig("ollama", {"model": "glm-ocr"}),
        )
        save_config(config)
        loaded = load_config(tmp_path)
        assert loaded.content_extractor is not None
        assert loaded.content_extractor.name == "ollama"
        assert loaded.content_extractor.params.get("model") == "glm-ocr"

    def test_save_without_extractor(self, tmp_path):
        from keep.config import StoreConfig, save_config

        config = StoreConfig(
            path=tmp_path,
            config_dir=tmp_path,
        )
        save_config(config)
        # Verify TOML doesn't contain a content_extractor section
        toml_text = (tmp_path / "keep.toml").read_text()
        assert "content_extractor" not in toml_text


# ---------------------------------------------------------------------------
# Pending queue: OCR stale claim timeout
# ---------------------------------------------------------------------------


class TestOcrStaleClaimTimeout:
    """Test that OCR task type has a stale claim timeout."""

    def test_ocr_stale_claim_timeout(self):
        from keep.pending_summaries import STALE_CLAIM_SECONDS_BY_TYPE
        assert "ocr" in STALE_CLAIM_SECONDS_BY_TYPE
        assert STALE_CLAIM_SECONDS_BY_TYPE["ocr"] > 0
