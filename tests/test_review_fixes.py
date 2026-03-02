"""
Tests for code review fixes: tag queries, SSRF protection, missing embedding provider.
"""

import os
import pytest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

from keep.document_store import DocumentStore
from keep.types import tag_values


# ---------------------------------------------------------------------------
# DocumentStore JSON tag queries (json_each / json_extract)
# ---------------------------------------------------------------------------


class TestTagQueries:
    """Tests for list_distinct_tag_keys and list_distinct_tag_values using json_each."""

    @pytest.fixture
    def store(self):
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "documents.db"
            with DocumentStore(db_path) as store:
                yield store

    def test_list_distinct_tag_keys_basic(self, store: DocumentStore) -> None:
        """Returns all user tag keys, sorted."""
        store.upsert("default", "d1", "S1", {"topic": "auth", "project": "web"})
        store.upsert("default", "d2", "S2", {"topic": "db", "status": "open"})

        keys = store.list_distinct_tag_keys("default")
        assert keys == ["project", "status", "topic"]

    def test_list_distinct_tag_keys_excludes_system(self, store: DocumentStore) -> None:
        """System tags (prefixed with _) are excluded."""
        store.upsert("default", "d1", "S1", {
            "topic": "auth",
            "_created": "2026-01-01",
            "_source": "inline",
        })

        keys = store.list_distinct_tag_keys("default")
        assert keys == ["topic"]
        assert "_created" not in keys
        assert "_source" not in keys

    def test_list_distinct_tag_keys_empty_collection(self, store: DocumentStore) -> None:
        """Empty collection returns empty list."""
        keys = store.list_distinct_tag_keys("default")
        assert keys == []

    def test_list_distinct_tag_keys_no_duplicates(self, store: DocumentStore) -> None:
        """Same key across multiple documents appears once."""
        store.upsert("default", "d1", "S1", {"topic": "a"})
        store.upsert("default", "d2", "S2", {"topic": "b"})
        store.upsert("default", "d3", "S3", {"topic": "c"})

        keys = store.list_distinct_tag_keys("default")
        assert keys.count("topic") == 1

    def test_list_distinct_tag_keys_collection_isolation(self, store: DocumentStore) -> None:
        """Keys from other collections are not included."""
        store.upsert("coll1", "d1", "S1", {"alpha": "1"})
        store.upsert("coll2", "d2", "S2", {"beta": "2"})

        assert store.list_distinct_tag_keys("coll1") == ["alpha"]
        assert store.list_distinct_tag_keys("coll2") == ["beta"]

    def test_list_distinct_tag_values_basic(self, store: DocumentStore) -> None:
        """Returns all distinct values for a key, sorted."""
        store.upsert("default", "d1", "S1", {"topic": "auth"})
        store.upsert("default", "d2", "S2", {"topic": "db"})
        store.upsert("default", "d3", "S3", {"topic": "auth"})  # duplicate

        values = store.list_distinct_tag_values("default", "topic")
        assert values == ["auth", "db"]

    def test_list_distinct_tag_values_missing_key(self, store: DocumentStore) -> None:
        """Key not present in any document returns empty list."""
        store.upsert("default", "d1", "S1", {"topic": "auth"})

        values = store.list_distinct_tag_values("default", "nonexistent")
        assert values == []

    def test_list_distinct_tag_values_partial_key(self, store: DocumentStore) -> None:
        """Only documents with the key contribute values."""
        store.upsert("default", "d1", "S1", {"topic": "auth", "status": "open"})
        store.upsert("default", "d2", "S2", {"topic": "db"})  # no status

        values = store.list_distinct_tag_values("default", "status")
        assert values == ["open"]

    def test_query_by_tag_key(self, store: DocumentStore) -> None:
        """query_by_tag_key returns documents having the specified key."""
        store.upsert("default", "d1", "S1", {"topic": "auth"})
        store.upsert("default", "d2", "S2", {"project": "web"})
        store.upsert("default", "d3", "S3", {"topic": "db", "project": "api"})

        results = store.query_by_tag_key("default", "topic")
        ids = {r.id for r in results}
        assert ids == {"d1", "d3"}

    def test_query_by_id_prefix_escapes_wildcards(self, store: DocumentStore) -> None:
        """LIKE wildcards in prefix are escaped, not treated as patterns."""
        store.upsert("default", "normal:1", "S1", {})
        store.upsert("default", "normal:2", "S2", {})
        store.upsert("default", "has%wild", "S3", {})
        store.upsert("default", "has_wild", "S4", {})

        # A prefix of "%" should NOT match everything
        results = store.query_by_id_prefix("default", "%")
        assert len(results) == 0  # no IDs start with literal %

        # A prefix of "_" should NOT match single-char wildcard
        results = store.query_by_id_prefix("default", "_")
        assert len(results) == 0

        # Literal prefix match works
        results = store.query_by_id_prefix("default", "normal:")
        assert len(results) == 2

        # Prefix with literal % matches the doc that has it
        results = store.query_by_id_prefix("default", "has%")
        assert len(results) == 1
        assert results[0].id == "has%wild"


# ---------------------------------------------------------------------------
# HTTP provider SSRF protection (is_private_url)
# ---------------------------------------------------------------------------


class TestIsPrivateUrl:
    """Tests for HttpDocumentProvider._is_private_url SSRF protection."""

    @pytest.fixture
    def provider(self):
        from keep.providers.documents import HttpDocumentProvider
        return HttpDocumentProvider()

    def test_loopback_ipv4(self, provider) -> None:
        assert provider._is_private_url("http://127.0.0.1/secret") is True

    def test_loopback_ipv6(self, provider) -> None:
        assert provider._is_private_url("http://[::1]/secret") is True

    def test_private_10_range(self, provider) -> None:
        assert provider._is_private_url("http://10.0.0.1/internal") is True

    def test_private_172_range(self, provider) -> None:
        assert provider._is_private_url("http://172.16.0.1/internal") is True

    def test_private_192_range(self, provider) -> None:
        assert provider._is_private_url("http://192.168.1.1/internal") is True

    def test_link_local(self, provider) -> None:
        assert provider._is_private_url("http://169.254.169.254/metadata") is True

    def test_cloud_metadata_endpoint(self, provider) -> None:
        assert provider._is_private_url("http://metadata.google.internal/v1") is True

    def test_no_hostname(self, provider) -> None:
        """URLs without a hostname are blocked."""
        assert provider._is_private_url("http:///path") is True

    def test_public_ip(self, provider) -> None:
        assert provider._is_private_url("http://8.8.8.8/dns") is False

    def test_public_domain(self, provider) -> None:
        """Real public domains are allowed."""
        assert provider._is_private_url("https://example.com/page") is False

    def test_localhost_name(self, provider) -> None:
        """localhost resolves to 127.0.0.1, should be blocked."""
        assert provider._is_private_url("http://localhost/secret") is True

    def test_unspecified_address(self, provider) -> None:
        """0.0.0.0 (unspecified) should be blocked."""
        assert provider._is_private_url("http://0.0.0.0/path") is True

    def test_multicast_address(self, provider) -> None:
        """Multicast addresses should be blocked."""
        assert provider._is_private_url("http://224.0.0.1/path") is True

    def test_fetch_blocks_private(self, provider) -> None:
        """fetch() raises IOError for private URLs."""
        with pytest.raises(IOError, match="private"):
            provider.fetch("http://127.0.0.1/secret")

    def test_fetch_blocks_redirect_to_private(self, provider) -> None:
        """fetch() blocks redirects to private addresses."""
        import requests

        mock_resp = MagicMock()
        mock_resp.is_redirect = True
        mock_resp.headers = {"Location": "http://127.0.0.1/internal"}
        mock_resp.close = MagicMock()

        with patch("requests.get", return_value=mock_resp):
            with pytest.raises(IOError, match="private"):
                provider.fetch("https://example.com/redirect")


# ---------------------------------------------------------------------------
# Embedding provider absent scenarios
# ---------------------------------------------------------------------------


class TestEmbeddingProviderAbsent:
    """Tests for behavior when no embedding provider is configured."""

    def test_get_embedding_provider_raises_with_message(self, tmp_path) -> None:
        """_get_embedding_provider raises RuntimeError with install instructions."""
        from keep.api import Keeper
        from keep.config import StoreConfig

        config = StoreConfig(path=tmp_path, embedding=None)

        with patch("keep.api.load_or_create_config", return_value=config), \
             patch("keep.store.ChromaStore"), \
             patch("keep.document_store.DocumentStore"), \
             patch("keep.pending_summaries.PendingSummaryQueue"):
            kp = Keeper(store_path=tmp_path)
            with pytest.raises(RuntimeError, match="No embedding provider configured"):
                kp._get_embedding_provider()

    def test_error_message_includes_install_options(self, tmp_path) -> None:
        """Error message mentions pip install and API key options."""
        from keep.api import Keeper
        from keep.config import StoreConfig

        config = StoreConfig(path=tmp_path, embedding=None)

        with patch("keep.api.load_or_create_config", return_value=config), \
             patch("keep.store.ChromaStore"), \
             patch("keep.document_store.DocumentStore"), \
             patch("keep.pending_summaries.PendingSummaryQueue"):
            kp = Keeper(store_path=tmp_path)
            try:
                kp._get_embedding_provider()
            except RuntimeError as e:
                msg = str(e)
                assert "keep-skill[local]" in msg
                assert "VOYAGE_API_KEY" in msg

    def test_store_config_accepts_none_embedding(self) -> None:
        """StoreConfig can be created with embedding=None."""
        from keep.config import StoreConfig
        config = StoreConfig(path=Path("/tmp/test"), embedding=None)
        assert config.embedding is None
        assert config.summarization.name == "truncate"  # default still works

    def test_save_config_handles_none_embedding(self, tmp_path) -> None:
        """save_config doesn't crash when embedding is None."""
        from keep.config import StoreConfig, save_config

        config = StoreConfig(path=tmp_path, config_dir=tmp_path, embedding=None)
        # Should not raise
        save_config(config)

        # Verify config file exists and doesn't have embedding section
        config_file = tmp_path / "keep.toml"
        assert config_file.exists()
        content = config_file.read_text()
        assert "embedding" not in content or "# embedding" in content


# ---------------------------------------------------------------------------
# File birthtime as created_at
# ---------------------------------------------------------------------------


class TestFileBirthtime:
    """File creation time should be used as created_at for file:// URIs."""

    @pytest.fixture
    def home_tmp(self):
        """Temp dir under home so FileDocumentProvider's safety check passes."""
        d = Path.home() / ".keep-test-birthtime"
        d.mkdir(exist_ok=True)
        yield d
        import shutil
        shutil.rmtree(d, ignore_errors=True)

    def test_file_provider_includes_birthtime(self, home_tmp):
        """FileDocumentProvider.fetch() includes birthtime in metadata."""
        from keep.providers.documents import FileDocumentProvider

        f = home_tmp / "note.md"
        f.write_text("hello")
        provider = FileDocumentProvider()
        doc = provider.fetch(str(f))
        # macOS always has st_birthtime; skip on platforms that don't
        if hasattr(os.stat_result, "st_birthtime"):
            assert "birthtime" in doc.metadata
            assert isinstance(doc.metadata["birthtime"], float)
        else:
            # On platforms without birthtime, key should be absent
            assert "birthtime" not in doc.metadata

    def test_put_file_uses_birthtime_as_created(self, mock_providers, home_tmp):
        """put() with a file:// URI sets _created from file birthtime."""
        from keep.api import Keeper
        from keep.providers.documents import FileDocumentProvider

        f = home_tmp / "old-note.md"
        f.write_text("historical content")

        kp = Keeper(store_path=home_tmp / "store")
        kp._document_provider = FileDocumentProvider()

        before = datetime.now(timezone.utc)
        item = kp.put(uri=f"file://{f}")

        if hasattr(os.stat_result, "st_birthtime"):
            created_str = item.tags["_created"]
            created = datetime.fromisoformat(created_str)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            # File was just created, so birthtime should be before 'now'
            # and close to it (within a few seconds)
            assert created < before or (created - before).total_seconds() < 2
        # Without birthtime, _created falls back to current time (existing behavior)

    def test_put_file_explicit_created_at_wins(self, mock_providers, home_tmp):
        """Explicit created_at overrides file birthtime."""
        from keep.api import Keeper
        from keep.providers.documents import FileDocumentProvider

        f = home_tmp / "note.md"
        f.write_text("content")

        kp = Keeper(store_path=home_tmp / "store")
        kp._document_provider = FileDocumentProvider()

        explicit = "2020-01-01T00:00:00+00:00"
        item = kp.put(uri=f"file://{f}", created_at=explicit)
        assert item.tags["_created"].startswith("2020-01-01")


# ---------------------------------------------------------------------------
# Part immutability
# ---------------------------------------------------------------------------


class TestIsPartId:
    """Unit tests for is_part_id helper."""

    def test_internal_format(self):
        from keep.types import is_part_id
        assert is_part_id("doc@p3") is True
        assert is_part_id("my-note@p12") is True

    def test_display_format(self):
        from keep.types import is_part_id
        assert is_part_id("doc@P{3}") is True
        assert is_part_id("my-note@P{12}") is True

    def test_not_part_id(self):
        from keep.types import is_part_id
        assert is_part_id("doc") is False
        assert is_part_id(".conversations") is False
        assert is_part_id("now") is False
        assert is_part_id("user@example.com") is False

    def test_at_sign_but_not_part(self):
        from keep.types import is_part_id
        # @ followed by non-p character
        assert is_part_id("doc@v3") is False


class TestPartImmutability:
    """Parts cannot be put, deleted, reverted, or moved."""

    def test_put_rejects_part_id(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)
        with pytest.raises(ValueError, match="Cannot modify part directly"):
            kp.put("content", id="doc@p3")

    def test_put_rejects_display_format(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)
        with pytest.raises(ValueError, match="Cannot modify part directly"):
            kp.put("content", id="doc@P{3}")

    def test_delete_rejects_part_id(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)
        with pytest.raises(ValueError, match="Cannot delete part directly"):
            kp.delete("doc@p3")

    def test_revert_rejects_part_id(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)
        with pytest.raises(ValueError, match="Cannot revert part directly"):
            kp.revert("doc@p3")

    def test_move_rejects_part_name(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)
        # Guard fires before any source lookup
        with pytest.raises(ValueError, match="Cannot move to a part ID"):
            kp.move("target@p3", only_current=True)


class TestTagPart:
    """Tag editing on parts."""

    def test_tag_part_not_found(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)
        result = kp.tag_part("nonexistent", 1, tags={"topic": "test"})
        assert result is None

    def test_tag_part_updates_tags(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.document_store import PartInfo
        kp = Keeper(store_path=tmp_path)

        # Create a doc and manually insert a part
        kp.put("test content", id="test-doc")
        doc_coll = kp._resolve_doc_collection()
        part = PartInfo(
            part_num=1,
            summary="test part",
            tags={"topic": "original"},
            content="test content",
            created_at="2026-01-01T00:00:00",
        )
        kp._document_store.upsert_parts(doc_coll, "test-doc", [part])

        # Update tags
        result = kp.tag_part("test-doc", 1, tags={"topic": "updated", "project": "myapp"})
        assert result is not None
        assert set(result.tags["topic"]) == {"original", "updated"}
        assert result.tags["project"] == "myapp"

    def test_tag_part_delete_with_empty_string(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.document_store import PartInfo
        kp = Keeper(store_path=tmp_path)

        kp.put("test content", id="test-doc2")
        doc_coll = kp._resolve_doc_collection()
        part = PartInfo(
            part_num=1,
            summary="test part",
            tags={"topic": "auth", "project": "myapp"},
            content="test content",
            created_at="2026-01-01T00:00:00",
        )
        kp._document_store.upsert_parts(doc_coll, "test-doc2", [part])

        # Remove topic tag
        result = kp.tag_part("test-doc2", 1, tags={"topic": ""})
        assert result is not None
        assert "topic" not in result.tags
        assert result.tags["project"] == "myapp"

    def test_tag_part_skips_system_tags(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.document_store import PartInfo
        kp = Keeper(store_path=tmp_path)

        kp.put("test content", id="test-doc3")
        doc_coll = kp._resolve_doc_collection()
        part = PartInfo(
            part_num=1,
            summary="test part",
            tags={"_part_num": "1", "topic": "auth"},
            content="test content",
            created_at="2026-01-01T00:00:00",
        )
        kp._document_store.upsert_parts(doc_coll, "test-doc3", [part])

        # Try to modify system tag — should be ignored
        result = kp.tag_part("test-doc3", 1, tags={"_part_num": "99", "topic": "updated"})
        assert result is not None
        assert result.tags["_part_num"] == "1"  # unchanged
        assert set(result.tags["topic"]) == {"auth", "updated"}

    def test_tag_part_remove_single_value(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.document_store import PartInfo
        kp = Keeper(store_path=tmp_path)

        kp.put("test content", id="test-doc4")
        doc_coll = kp._resolve_doc_collection()
        part = PartInfo(
            part_num=1,
            summary="test part",
            tags={"speaker": ["Alice", "Bob"]},
            content="test content",
            created_at="2026-01-01T00:00:00",
        )
        kp._document_store.upsert_parts(doc_coll, "test-doc4", [part])

        result = kp.tag_part("test-doc4", 1, remove_values={"speaker": "Bob"})
        assert result is not None
        assert tag_values(result.tags, "speaker") == ["Alice"]


class TestTagMutations:
    """Value-level tag mutation behavior for Keeper.tag()."""

    def test_tag_remove_single_value(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)

        kp.put("meeting notes", id="doc:tag:1", tags={"speaker": ["Alice", "Bob"]})
        result = kp.tag("doc:tag:1", remove_values={"speaker": "Bob"})
        assert result is not None
        assert tag_values(result.tags, "speaker") == ["Alice"]

    def test_tag_remove_and_add_in_one_call(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)

        kp.put("meeting notes", id="doc:tag:2", tags={"speaker": ["Alice", "Bob"]})
        result = kp.tag(
            "doc:tag:2",
            tags={"speaker": "Carol"},
            remove_values={"speaker": "Bob"},
        )
        assert result is not None
        assert set(tag_values(result.tags, "speaker")) == {"Alice", "Carol"}

    def test_tag_add_literal_dash_prefix_value(self, mock_providers, tmp_path):
        from keep.api import Keeper
        kp = Keeper(store_path=tmp_path)

        kp.put("meeting notes", id="doc:tag:3", tags={"speaker": "Alice"})
        result = kp.tag("doc:tag:3", tags={"speaker": "-Bob"})
        assert result is not None
        assert set(tag_values(result.tags, "speaker")) == {"Alice", "-Bob"}

    def test_tag_removal_skips_constrained_validation(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.types import utc_now
        kp = Keeper(store_path=tmp_path)
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()

        # Constrained tag setup: only status=open is valid.
        kp._document_store.upsert(
            doc_coll, ".tag/status", summary="status",
            tags={"_constrained": "true", "_created": now, "_updated": now, "_source": "inline"},
        )
        kp._document_store.upsert(
            doc_coll, ".tag/status/open", summary="open",
            tags={"_created": now, "_updated": now, "_source": "inline"},
        )

        kp.put("item", id="doc:tag:4", tags={"status": "open"})

        # Removing a value should not require the removed token to be a valid constrained value.
        result = kp.tag("doc:tag:4", remove_values={"status": "closed"})
        assert result is not None
        assert tag_values(result.tags, "status") == ["open"]

        # Adding an invalid constrained value should still fail.
        with pytest.raises(ValueError, match="Invalid value for constrained tag"):
            kp.tag("doc:tag:4", tags={"status": "closed"})


class TestVersionContextNavigation:
    """Regression tests for offset-based version navigation in get_context()."""

    def test_get_context_old_version_uses_offset_neighbors(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.types import Item

        kp = Keeper(store_path=tmp_path)

        # Current exists but should not be used for offset>1 next link.
        kp.get = lambda _id: Item(id="doc", summary="current", tags={"_created": "2026-01-04T00:00:00+00:00"})  # type: ignore[method-assign]

        # Simulate sparse internal version numbering behind a stable offset API.
        by_offset = {
            2: Item(id="doc", summary="viewed", tags={"_created": "2026-01-03T00:00:00+00:00", "_version": "100"}),
            3: Item(id="doc", summary="older", tags={"_created": "2026-01-02T00:00:00+00:00", "_version": "42"}),
            1: Item(id="doc", summary="newer", tags={"_created": "2026-01-04T00:00:00+00:00", "_version": "777"}),
        }
        kp.get_version = lambda _id, off=0: by_offset.get(off)  # type: ignore[method-assign]

        # Guard against regressions that pass offset into internal-version nav.
        kp.get_version_nav = lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not call get_version_nav for old-version context"))  # type: ignore[method-assign]

        ctx = kp.get_context(
            "doc",
            version=2,
            include_similar=False,
            include_meta=False,
            include_parts=False,
            include_versions=True,
        )

        assert ctx is not None
        assert [v.offset for v in ctx.prev] == [3]
        assert [v.summary for v in ctx.prev] == ["older"]
        assert [v.offset for v in ctx.next] == [1]
        assert [v.summary for v in ctx.next] == ["newer"]


class TestTagMarkerMigration:
    """Legacy Chroma key/value metadata is rewritten to marker metadata."""

    def test_migrates_legacy_tag_metadata_in_place(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.types import casefold_tags_for_index

        kp = Keeper(store_path=tmp_path)
        try:
            kp.put("hello world", id="doc:legacy", tags={"topic": "auth"})
            doc_coll = kp._resolve_doc_collection()
            chroma_coll = kp._resolve_chroma_collection()
            doc = kp._document_store.get(doc_coll, "doc:legacy")
            assert doc is not None
            # Ensure a searchable row exists in the vector index.
            kp._store.upsert(
                chroma_coll,
                "doc:legacy",
                kp._get_embedding_provider().embed(doc.summary),
                doc.summary,
                casefold_tags_for_index(doc.tags),
            )

            # Force legacy metadata shape (no _tk::_tv:: markers).
            legacy_meta = {"topic": "auth", "_source": "inline"}
            kp._store._data[chroma_coll]["doc:legacy"]["metadata"] = legacy_meta

            assert not kp._store.has_tag_markers(chroma_coll, "doc:legacy")

            assert kp._needs_chroma_tag_marker_migration(chroma_coll, doc_coll)
            stats = kp._migrate_chroma_tag_markers(chroma_coll, doc_coll)
            assert stats["docs"] >= 1
            assert kp._store.has_tag_markers(chroma_coll, "doc:legacy")

            # Tag-filtered semantic path should work again.
            results = kp.find(
                similar_to="doc:legacy",
                tags={"topic": "auth"},
                include_self=True,
                limit=5,
            )
            assert any(r.id == "doc:legacy" for r in results)
        finally:
            kp.close()

    def test_detects_legacy_marker_gap_on_versions_and_parts(
        self, mock_providers, tmp_path,
    ):
        from keep.api import Keeper
        from keep.document_store import PartInfo
        from keep.types import casefold_tags_for_index, utc_now

        kp = Keeper(store_path=tmp_path)
        try:
            # Current document has no user tags.
            kp.put("body", id="doc:history")

            doc_coll = kp._resolve_doc_collection()
            chroma_coll = kp._resolve_chroma_collection()

            # Add one indexed part with user tags and then force legacy shape.
            part = PartInfo(
                part_num=1,
                summary="part summary",
                tags={"topic": "auth"},
                content="part body",
                created_at=utc_now(),
            )
            kp._document_store.upsert_parts(doc_coll, "doc:history", [part])
            part_id = "doc:history@p1"
            part_tags = {"topic": "auth", "_part_num": "1", "_base_id": "doc:history"}
            kp._store.upsert(
                chroma_coll,
                part_id,
                kp._get_embedding_provider().embed(part.summary),
                part.summary,
                casefold_tags_for_index(part_tags),
            )
            kp._store._data[chroma_coll][part_id]["metadata"] = {
                "topic": "auth",
                "_part_num": "1",
                "_base_id": "doc:history",
                "_source": "inline",
            }

            # Current doc has no user tags; part row should still trigger migration.
            current = kp.get("doc:history")
            assert current is not None
            assert "topic" not in current.tags
            assert kp._needs_chroma_tag_marker_migration(chroma_coll, doc_coll)
        finally:
            kp.close()


class TestNegativeVersionSelectors:
    """Public negative selectors resolve by oldest-ordinal semantics."""

    def test_resolve_version_offset_negative_from_oldest(self, mock_providers, tmp_path):
        from keep.api import Keeper

        kp = Keeper(store_path=tmp_path)
        kp._document_store.version_count = lambda _coll, _id: 5  # type: ignore[method-assign]

        assert kp.resolve_version_offset("doc", -1) == 5  # oldest
        assert kp.resolve_version_offset("doc", -2) == 4  # second-oldest
        assert kp.resolve_version_offset("doc", -5) == 1  # newest archived
        assert kp.resolve_version_offset("doc", -6) is None  # out of range

    def test_get_context_accepts_negative_selector(self, mock_providers, tmp_path):
        from keep.api import Keeper
        from keep.types import Item

        kp = Keeper(store_path=tmp_path)
        kp._document_store.version_count = lambda _coll, _id: 3  # type: ignore[method-assign]

        seen_offsets: list[int] = []

        def _get_version(_id: str, off: int = 0):
            seen_offsets.append(off)
            if off == 3:
                return Item(id="doc", summary="oldest", tags={"_created": "2026-01-01T00:00:00+00:00"})
            return None

        kp.get_version = _get_version  # type: ignore[method-assign]

        ctx = kp.get_context(
            "doc",
            version=-1,
            include_similar=False,
            include_meta=False,
            include_parts=False,
            include_versions=False,
        )
        assert ctx is not None
        assert ctx.item.summary == "oldest"
        assert seen_offsets == [3]
