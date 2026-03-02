"""Regression tests for consistent tag-key validation across write paths."""

import pytest

from keep.api import Keeper
from keep.config import ProviderConfig, StoreConfig
from keep.providers.base import Document
from keep.types import MAX_TAG_VALUES_PER_KEY


def test_invalid_default_tag_key_rejected_on_init(mock_providers, tmp_path):
    """Config default tags must satisfy the same key validation as write tags."""
    config = StoreConfig(
        path=tmp_path,
        config_dir=tmp_path,
        embedding=None,
        summarization=ProviderConfig("truncate"),
        document=ProviderConfig("composite"),
        default_tags={"bad!default": "x"},
    )
    with pytest.raises(ValueError, match="Config default tags"):
        Keeper(store_path=tmp_path, config=config)


def test_invalid_env_tag_key_rejected_on_init(mock_providers, tmp_path, monkeypatch):
    """KEEP_TAG_* keys are validated before any writes occur."""
    monkeypatch.setenv("KEEP_TAG_BAD!ENV", "x")
    with pytest.raises(ValueError, match="environment tags"):
        Keeper(store_path=tmp_path)


def test_invalid_frontmatter_tag_key_rejected(mock_providers, tmp_path):
    """Markdown frontmatter tags must satisfy the shared tag-key rule."""
    class MockMarkdownProvider:
        def fetch(self, uri: str) -> Document:
            return Document(
                uri=uri,
                content="---\nbad!front: yes\n---\nhello\n",
                content_type="text/markdown",
                metadata={},
                tags=None,
            )

    kp = Keeper(store_path=tmp_path)
    kp._document_provider = MockMarkdownProvider()
    try:
        with pytest.raises(ValueError, match="Frontmatter tags"):
            kp.put(uri="file:///mock.md")
    finally:
        kp.close()


def test_move_rejects_invalid_filter_key(mock_providers, tmp_path):
    """move(tags=...) uses the same tag-key validation as list/find filters."""
    kp = Keeper(store_path=tmp_path)
    try:
        with pytest.raises(ValueError, match="invalid characters"):
            kp.move("dest", tags={"bad!key": "x"})
    finally:
        kp.close()


def test_tag_update_rejects_too_many_values_with_source_context(
    mock_providers, tmp_path,
):
    """Tag mutation overflow errors include source context for CLI/API parity."""
    kp = Keeper(store_path=tmp_path)
    try:
        kp.put("x", id="doc:overflow")
        values = [f"v{i}" for i in range(MAX_TAG_VALUES_PER_KEY + 1)]
        with pytest.raises(ValueError, match="Tags: Too many distinct values"):
            kp.tag("doc:overflow", tags={"topic": values})
    finally:
        kp.close()
