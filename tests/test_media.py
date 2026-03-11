"""Tests for media description: state doc rules, dispatch, and task workflow."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from keep.config import ProviderConfig
from keep.providers.base import MediaDescriber, get_registry


# -----------------------------------------------------------------------------
# Protocol Tests
# -----------------------------------------------------------------------------

class TestMediaDescriberProtocol:
    """Verify MediaDescriber protocol compliance."""

    def test_protocol_is_runtime_checkable(self):
        class FakeDescriber:
            def describe(self, path: str, content_type: str) -> str | None:
                return "a test description"

        assert isinstance(FakeDescriber(), MediaDescriber)

    def test_non_conforming_class_fails_check(self):
        class NotADescriber:
            def summarize(self, content: str) -> str:
                return content

        assert not isinstance(NotADescriber(), MediaDescriber)


# -----------------------------------------------------------------------------
# Registry Tests
# -----------------------------------------------------------------------------

class TestMediaRegistry:

    def test_register_and_create(self):
        registry = get_registry()

        class TestDescriber:
            def __init__(self, greeting="hello"):
                self.greeting = greeting
            def describe(self, path: str, content_type: str) -> str | None:
                return f"{self.greeting}: {path}"

        registry.register_media("test-media", TestDescriber)
        try:
            describer = registry.create_media("test-media", {"greeting": "hi"})
            assert describer.describe("/img.jpg", "image/jpeg") == "hi: /img.jpg"
        finally:
            del registry._media_providers["test-media"]

    def test_create_unknown_raises(self):
        registry = get_registry()
        with pytest.raises(ValueError, match="Unknown media provider"):
            registry.create_media("nonexistent-provider")


# -----------------------------------------------------------------------------
# Config Tests
# -----------------------------------------------------------------------------

class TestMediaConfig:

    def test_config_media_defaults_to_none(self, tmp_path):
        from keep.config import StoreConfig
        config = StoreConfig(path=tmp_path)
        assert config.media is None

    def test_config_roundtrip_with_media(self, tmp_path):
        from keep.config import StoreConfig, ProviderConfig, save_config, load_config

        config = StoreConfig(
            path=tmp_path,
            config_dir=tmp_path,
            media=ProviderConfig("mlx", {
                "vision_model": "test-vision",
                "whisper_model": "test-whisper",
            }),
        )
        save_config(config)
        loaded = load_config(tmp_path)

        assert loaded.media is not None
        assert loaded.media.name == "mlx"
        assert loaded.media.params["vision_model"] == "test-vision"

    def test_config_roundtrip_without_media(self, tmp_path):
        from keep.config import StoreConfig, save_config, load_config

        config = StoreConfig(path=tmp_path, config_dir=tmp_path, media=None)
        save_config(config)
        with patch("keep.config._detect_ollama", return_value=None):
            loaded = load_config(tmp_path)

        assert loaded.media is None


# -----------------------------------------------------------------------------
# LockedMediaDescriber Tests
# -----------------------------------------------------------------------------

class TestLockedMediaDescriber:

    def test_locked_describer_delegates(self, tmp_path):
        from keep.model_lock import LockedMediaDescriber

        inner = MagicMock()
        inner.describe.return_value = "a cat"
        locked = LockedMediaDescriber(inner, tmp_path / ".media.lock")

        result = locked.describe("/test.jpg", "image/jpeg")
        assert result == "a cat"
        inner.describe.assert_called_once_with("/test.jpg", "image/jpeg")

    def test_locked_describer_release(self, tmp_path):
        from keep.model_lock import LockedMediaDescriber

        inner = MagicMock()
        locked = LockedMediaDescriber(inner, tmp_path / ".media.lock")
        locked.release()
        assert locked._provider is None


# -----------------------------------------------------------------------------
# After-write state doc rule evaluation
# -----------------------------------------------------------------------------

class TestAfterWriteStateDoc:
    """Verify the after-write state doc rules are the source of truth
    for which background tasks fire after put().

    These tests evaluate the state doc directly — no Keeper, no mocks.
    If a rule is wrong here, the entire post-write pipeline is wrong.
    """

    @pytest.fixture
    def after_write_doc(self):
        from keep.state_doc import parse_state_doc
        from keep.builtin_state_docs import BUILTIN_STATE_DOCS
        return parse_state_doc("after-write", BUILTIN_STATE_DOCS["after-write"])

    def _eval(self, doc, system=None, **item_overrides):
        from keep.state_doc import evaluate_state_doc
        item = {
            "content_length": 50,
            "has_summary": False,
            "has_uri": False,
            "is_system_note": False,
            "tags": {},
            "has_media_content": False,
            "has_content": True,
        }
        item.update(item_overrides)
        sys = {"has_media_provider": True}
        if system is not None:
            sys.update(system)
        ctx = {"item": item, "params": {"max_summary_length": 2000}, "system": sys}
        result = evaluate_state_doc(doc, ctx, run_action=None)
        return [a["action"] for a in result.actions]

    def test_inline_text_fires_analyze_and_tag(self, after_write_doc):
        """Short inline text → analyze + tag only."""
        actions = self._eval(after_write_doc)
        assert actions == ["analyze", "tag"]

    def test_long_content_fires_summarize(self, after_write_doc):
        """Content exceeding max_summary_length fires summarize."""
        actions = self._eval(after_write_doc, content_length=5000)
        assert "summarize" in actions

    def test_system_note_skips_analyze_and_tag(self, after_write_doc):
        """System notes (dot-prefix IDs) skip analyze and tag."""
        actions = self._eval(after_write_doc, is_system_note=True)
        assert "analyze" not in actions
        assert "tag" not in actions

    def test_image_uri_fires_describe(self, after_write_doc):
        """URI-backed image content fires describe."""
        actions = self._eval(after_write_doc,
                             has_uri=True, has_media_content=True)
        assert "describe" in actions

    def test_text_uri_skips_describe(self, after_write_doc):
        """URI-backed text content does NOT fire describe."""
        actions = self._eval(after_write_doc,
                             has_uri=True, has_media_content=False)
        assert "describe" not in actions

    def test_ocr_pages_fires_ocr(self, after_write_doc):
        """Items with _ocr_pages tag and URI fire OCR."""
        actions = self._eval(after_write_doc,
                             has_uri=True, tags={"_ocr_pages": "[1,2]"})
        assert "ocr" in actions

    def test_no_content_skips_tag(self, after_write_doc):
        """Empty content skips tag (nothing to classify)."""
        actions = self._eval(after_write_doc, has_content=False)
        assert "tag" not in actions
        assert "analyze" in actions  # analyze still fires


# -----------------------------------------------------------------------------
# Integration: put() → state doc → work queue
# -----------------------------------------------------------------------------

def _make_mock_doc(uri, content, content_type, tags=None, metadata=None):
    """Create a mock Document for testing."""
    mock_doc = MagicMock()
    mock_doc.uri = uri
    mock_doc.content = content
    mock_doc.content_type = content_type
    mock_doc.metadata = metadata
    mock_doc.tags = tags
    return mock_doc


def _keeper_skip_migration(kp):
    """Skip system docs migration for test Keepers."""
    from keep.system_docs import _bundled_docs_hash
    kp._config.system_docs_hash = _bundled_docs_hash()
    kp._needs_sysdoc_migration = False


def _claimed_task_kinds(kp, limit=20):
    """Claim work queue items and return their kinds as a list."""
    claimed = kp._work_queue.claim("test", limit=limit)
    return [t.kind for t in claimed]


class TestAfterWriteDispatch:
    """Verify _dispatch_after_write_flow enqueues the right tasks
    through the full put() → state doc → work queue path.
    """

    def test_image_put_enqueues_describe_analyze_tag(self, mock_providers, tmp_path):
        """Image URI with media config → describe + analyze + tag tasks."""
        from keep.api import Keeper

        mock_doc = _make_mock_doc(
            "file:///test.jpg",
            "Dimensions: 1920x1080\nCamera: Canon EOS R5",
            "image/jpeg",
            tags={"dimensions": "1920x1080"},
        )
        mock_providers["document"].fetch = lambda uri: mock_doc

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)
        kp._config.media = ProviderConfig("ollama", {"model": "test"})

        kp.put(uri="file:///test.jpg")

        kinds = _claimed_task_kinds(kp)
        assert "describe" in kinds
        assert "analyze" in kinds
        assert "tag" in kinds
        kp.close()

    def test_image_put_without_media_config_skips_describe(self, mock_providers, tmp_path):
        """Image URI without media config → no describe task (capability gate)."""
        from keep.api import Keeper

        mock_doc = _make_mock_doc(
            "file:///test.jpg", "Dimensions: 100x100", "image/jpeg",
        )
        mock_providers["document"].fetch = lambda uri: mock_doc

        with patch("keep.config._detect_ollama", return_value=None):
            kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)
        assert kp._config.media is None

        kp.put(uri="file:///test.jpg")

        kinds = _claimed_task_kinds(kp)
        assert "describe" not in kinds
        # analyze + tag still fire (not gated by media config)
        assert "analyze" in kinds
        assert "tag" in kinds
        kp.close()

    def test_audio_put_enqueues_describe(self, mock_providers, tmp_path):
        """Audio URI with media config → describe task."""
        from keep.api import Keeper

        mock_doc = _make_mock_doc(
            "file:///test.mp3", "Title: Song\nArtist: Band", "audio/mpeg",
            tags={"title": "Song"},
        )
        mock_providers["document"].fetch = lambda uri: mock_doc

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)
        kp._config.media = ProviderConfig("ollama", {"model": "test"})

        kp.put(uri="file:///test.mp3")

        kinds = _claimed_task_kinds(kp)
        assert "describe" in kinds
        kp.close()

    def test_text_uri_skips_describe(self, mock_providers, tmp_path):
        """Text/markdown URI → no describe, still analyze + tag."""
        from keep.api import Keeper

        mock_doc = _make_mock_doc(
            "file:///test.md", "# Hello World", "text/markdown",
        )
        mock_providers["document"].fetch = lambda uri: mock_doc

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)
        kp._config.media = ProviderConfig("ollama", {"model": "test"})

        kp.put(uri="file:///test.md")

        kinds = _claimed_task_kinds(kp)
        assert "describe" not in kinds
        assert "analyze" in kinds
        kp.close()

    def test_inline_text_enqueues_analyze_and_tag(self, mock_providers, tmp_path):
        """Inline text → analyze + tag, no describe or OCR."""
        from keep.api import Keeper

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)

        kp.put("A note about architecture", id="note1")

        kinds = _claimed_task_kinds(kp)
        assert "analyze" in kinds
        assert "tag" in kinds
        assert "describe" not in kinds
        assert "ocr" not in kinds
        kp.close()

    def test_system_note_enqueues_nothing(self, mock_providers, tmp_path):
        """System note (dot-prefix) → no analyze, no tag."""
        from keep.api import Keeper

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)

        kp.put("System data", id=".sys/test")

        # Work queue may not even be initialized (no tasks to dispatch)
        if kp._work_queue is not None:
            kinds = _claimed_task_kinds(kp)
            assert "analyze" not in kinds
            assert "tag" not in kinds
        kp.close()


# -----------------------------------------------------------------------------
# Describe task workflow
# -----------------------------------------------------------------------------

class TestDescribeTaskWorkflow:
    """Test the describe task workflow (background execution path)."""

    def test_describe_enriches_summary(self, mock_providers, tmp_path):
        """Describe appends description to existing summary."""
        from keep.api import Keeper
        from keep.task_workflows import TaskRequest, run_local_task

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)

        # Create item with initial summary
        kp.put("Dimensions: 100x100", id="img1")

        # Set up a mock media describer
        mock_describer = MagicMock()
        mock_describer.describe.return_value = "A photo of a sunset"
        kp._media_describer = mock_describer
        kp._config.media = ProviderConfig("ollama", {"model": "test"})

        # Create a file for the describer to find
        test_file = tmp_path / "test.jpg"
        test_file.write_bytes(b"fake image data")

        req = TaskRequest(
            task_type="describe",
            id="img1",
            collection=kp._resolve_doc_collection(),
            content="",
            metadata={"uri": str(test_file), "content_type": "image/jpeg"},
        )
        # Patch path validation (tmp_path is outside home)
        with patch("keep.actions.describe.validate_path_within_home"):
            result = run_local_task(kp, req)

        assert result.status == "applied"
        # Verify the summary was enriched
        item = kp.get("img1")
        assert "Description:" in item.summary
        assert "A photo of a sunset" in item.summary
        kp.close()

    def test_describe_skips_when_no_provider(self, mock_providers, tmp_path):
        """Describe skips gracefully without a media provider."""
        from keep.api import Keeper
        from keep.task_workflows import TaskRequest, run_local_task

        with patch("keep.config._detect_ollama", return_value=None):
            kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)
        kp.put("Dimensions: 100x100", id="img1")

        req = TaskRequest(
            task_type="describe",
            id="img1",
            collection=kp._resolve_doc_collection(),
            content="",
            metadata={"uri": "/test.jpg", "content_type": "image/jpeg"},
        )
        result = run_local_task(kp, req)
        assert result.status == "skipped"
        kp.close()

    def test_describe_skips_on_empty_description(self, mock_providers, tmp_path):
        """Describe skips when describer returns empty string."""
        from keep.api import Keeper
        from keep.task_workflows import TaskRequest, run_local_task

        kp = Keeper(store_path=tmp_path)
        _keeper_skip_migration(kp)
        kp.put("Original summary", id="img1")

        mock_describer = MagicMock()
        mock_describer.describe.return_value = "   "
        kp._media_describer = mock_describer
        kp._config.media = ProviderConfig("ollama", {"model": "test"})

        test_file = tmp_path / "test.jpg"
        test_file.write_bytes(b"fake")

        req = TaskRequest(
            task_type="describe",
            id="img1",
            collection=kp._resolve_doc_collection(),
            content="",
            metadata={"uri": str(test_file), "content_type": "image/jpeg"},
        )
        with patch("keep.actions.describe.validate_path_within_home"):
            result = run_local_task(kp, req)
        assert result.status == "skipped"

        # Original summary unchanged
        item = kp.get("img1")
        assert "Description:" not in item.summary
        kp.close()


# -----------------------------------------------------------------------------
# MLX Describer Unit Tests (no real models)
# -----------------------------------------------------------------------------

class TestMLXMediaDescriber:

    def test_returns_none_for_text(self):
        from keep.providers.mlx import MLXMediaDescriber

        describer = object.__new__(MLXMediaDescriber)
        describer._vision = None
        describer._whisper = None
        describer._vision_checked = True
        describer._whisper_checked = True

        assert describer.describe("/test.txt", "text/plain") is None

    def test_image_delegates_to_vision(self):
        from keep.providers.mlx import MLXMediaDescriber

        describer = object.__new__(MLXMediaDescriber)
        mock_vision = MagicMock()
        mock_vision.describe.return_value = "A cat"
        describer._vision = mock_vision
        describer._vision_checked = True
        describer._whisper = None
        describer._whisper_checked = True

        result = describer.describe("/cat.jpg", "image/jpeg")
        assert result == "A cat"
        mock_vision.describe.assert_called_once_with("/cat.jpg", "image/jpeg")

    def test_audio_delegates_to_whisper(self):
        from keep.providers.mlx import MLXMediaDescriber

        describer = object.__new__(MLXMediaDescriber)
        describer._vision = None
        describer._vision_checked = True
        mock_whisper = MagicMock()
        mock_whisper.describe.return_value = "Hello world"
        describer._whisper = mock_whisper
        describer._whisper_checked = True

        result = describer.describe("/speech.mp3", "audio/mpeg")
        assert result == "Hello world"
        mock_whisper.describe.assert_called_once_with("/speech.mp3", "audio/mpeg")
