"""
Tests for document parts (structural decomposition).

Tests cover:
- DocumentStore CRUD for parts
- Schema migration (version 3→4)
- ChromaStore part methods
- Keeper.analyze() with mocked LLM
- Keeper.get_part() and list_parts()
- CLI @P{N} parsing
- Parts manifest in get output
- JSON decomposition parsing
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from keep.api import Keeper
from keep.analyzers import _parse_decomposition_json
from keep.document_store import DocumentStore, PartInfo
from keep.types import utc_now


# ---------------------------------------------------------------------------
# DocumentStore CRUD tests
# ---------------------------------------------------------------------------


class TestDocumentStoreParts:
    """Test PartInfo CRUD in the SQLite document store."""

    @pytest.fixture
    def store(self, tmp_path):
        db_path = tmp_path / "test.db"
        return DocumentStore(db_path)

    def test_upsert_and_list_parts(self, store):
        """Parts can be stored and retrieved."""
        now = utc_now()
        parts = [
            PartInfo(part_num=1, summary="Intro", tags={"topic": "overview"},
                     content="The introduction...", created_at=now),
            PartInfo(part_num=2, summary="Main body", tags={"topic": "detail"},
                     content="The main body...", created_at=now),
        ]
        count = store.upsert_parts("default", "doc:1", parts)
        assert count == 2

        result = store.list_parts("default", "doc:1")
        assert len(result) == 2
        assert result[0].part_num == 1
        assert result[0].summary == "Intro"
        assert result[0].tags == {"topic": "overview"}
        assert result[0].content == "The introduction..."
        assert result[1].part_num == 2

    def test_get_part(self, store):
        """Individual parts can be retrieved by number."""
        now = utc_now()
        parts = [
            PartInfo(part_num=1, summary="Part 1", tags={},
                     content="Content 1", created_at=now),
            PartInfo(part_num=2, summary="Part 2", tags={},
                     content="Content 2", created_at=now),
        ]
        store.upsert_parts("default", "doc:1", parts)

        part = store.get_part("default", "doc:1", 1)
        assert part is not None
        assert part.summary == "Part 1"

        part2 = store.get_part("default", "doc:1", 2)
        assert part2 is not None
        assert part2.summary == "Part 2"

        missing = store.get_part("default", "doc:1", 99)
        assert missing is None

    def test_part_count(self, store):
        """Part count returns correct number."""
        assert store.part_count("default", "doc:1") == 0

        now = utc_now()
        parts = [PartInfo(i, f"Part {i}", {}, f"Content {i}", now) for i in range(1, 4)]
        store.upsert_parts("default", "doc:1", parts)
        assert store.part_count("default", "doc:1") == 3

    def test_delete_parts(self, store):
        """Parts can be deleted."""
        now = utc_now()
        parts = [PartInfo(1, "Part 1", {}, "Content", now)]
        store.upsert_parts("default", "doc:1", parts)
        assert store.part_count("default", "doc:1") == 1

        deleted = store.delete_parts("default", "doc:1")
        assert deleted == 1
        assert store.part_count("default", "doc:1") == 0

    def test_upsert_replaces_atomically(self, store):
        """Re-upsert replaces all parts atomically."""
        now = utc_now()

        # Initial parts
        parts_v1 = [
            PartInfo(1, "Old part 1", {}, "Old 1", now),
            PartInfo(2, "Old part 2", {}, "Old 2", now),
            PartInfo(3, "Old part 3", {}, "Old 3", now),
        ]
        store.upsert_parts("default", "doc:1", parts_v1)
        assert store.part_count("default", "doc:1") == 3

        # Replace with fewer parts
        parts_v2 = [
            PartInfo(1, "New part 1", {"topic": "new"}, "New 1", now),
            PartInfo(2, "New part 2", {}, "New 2", now),
        ]
        store.upsert_parts("default", "doc:1", parts_v2)
        assert store.part_count("default", "doc:1") == 2

        result = store.list_parts("default", "doc:1")
        assert result[0].summary == "New part 1"
        assert result[0].tags == {"topic": "new"}

    def test_parts_isolated_by_id(self, store):
        """Parts for different documents are independent."""
        now = utc_now()
        store.upsert_parts("default", "doc:1", [PartInfo(1, "A", {}, "a", now)])
        store.upsert_parts("default", "doc:2", [PartInfo(1, "B", {}, "b", now)])

        assert store.part_count("default", "doc:1") == 1
        assert store.part_count("default", "doc:2") == 1

        store.delete_parts("default", "doc:1")
        assert store.part_count("default", "doc:1") == 0
        assert store.part_count("default", "doc:2") == 1

    def test_upsert_parts_deduplicates_tag_values(self, store):
        """upsert_parts() deduplicates multivalue tags per key."""
        now = utc_now()
        store.upsert_parts(
            "default",
            "doc:1",
            [PartInfo(1, "Part", {"k": ["a", "a", "b"]}, "Body", now)],
        )

        part = store.get_part("default", "doc:1", 1)
        assert part is not None
        assert part.tags == {"k": ["a", "b"]}

    def test_upsert_single_part_deduplicates_tag_values(self, store):
        """upsert_single_part() deduplicates multivalue tags per key."""
        now = utc_now()
        store.upsert_single_part(
            "default",
            "doc:1",
            PartInfo(1, "Part", {"k": ["a", "a", "b"]}, "Body", now),
        )

        part = store.get_part("default", "doc:1", 1)
        assert part is not None
        assert part.tags == {"k": ["a", "b"]}

    def test_update_part_tags_deduplicates_tag_values(self, store):
        """update_part_tags() deduplicates multivalue tags per key."""
        now = utc_now()
        store.upsert_parts(
            "default",
            "doc:1",
            [PartInfo(1, "Part", {"k": "a"}, "Body", now)],
        )

        updated = store.update_part_tags("default", "doc:1", 1, {"k": ["a", "a", "b"]})
        assert updated is True

        part = store.get_part("default", "doc:1", 1)
        assert part is not None
        assert part.tags == {"k": ["a", "b"]}


# ---------------------------------------------------------------------------
# Schema migration test
# ---------------------------------------------------------------------------


class TestSchemaMigration:
    """Test that schema v3→v4 migration creates the parts table."""

    def test_migration_creates_parts_table(self, tmp_path):
        """New databases get the document_parts table."""
        db_path = tmp_path / "test.db"
        store = DocumentStore(db_path)

        # Check table exists by inserting a part
        now = utc_now()
        parts = [PartInfo(1, "Test", {}, "Content", now)]
        store.upsert_parts("default", "test", parts)
        assert store.part_count("default", "test") == 1
        store.close()


# ---------------------------------------------------------------------------
# JSON parsing tests
# ---------------------------------------------------------------------------


class TestDecompositionParsing:
    """Test _parse_decomposition_json."""

    def test_parse_json_array(self):
        text = json.dumps([
            {"summary": "Intro", "content": "The intro text"},
            {"summary": "Body", "content": "The body text", "tags": {"topic": "main"}},
        ])
        result = _parse_decomposition_json(text)
        assert len(result) == 2
        assert result[0]["summary"] == "Intro"
        assert result[1]["tags"] == {"topic": "main"}

    def test_parse_code_fenced(self):
        text = '```json\n[{"summary": "Test", "content": "Content"}]\n```'
        result = _parse_decomposition_json(text)
        assert len(result) == 1
        assert result[0]["summary"] == "Test"

    def test_parse_wrapper_object(self):
        text = json.dumps({"sections": [
            {"summary": "Part 1", "content": "Text 1"},
        ]})
        result = _parse_decomposition_json(text)
        assert len(result) == 1
        assert result[0]["summary"] == "Part 1"

    def test_parse_empty_text(self):
        assert _parse_decomposition_json("") == []
        assert _parse_decomposition_json(None) == []

    def test_parse_invalid_json(self):
        assert _parse_decomposition_json("not json at all") == []

    def test_parse_skips_empty_entries(self):
        text = json.dumps([
            {"summary": "Good", "content": "Text"},
            {},  # No summary or content
            {"summary": "", "content": ""},  # Empty strings
        ])
        result = _parse_decomposition_json(text)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Keeper.analyze() tests (mocked providers)
# ---------------------------------------------------------------------------


class TestKeeperAnalyze:
    """Test Keeper.analyze() with mocked providers."""

    def test_analyze_creates_parts(self, mock_providers, tmp_path):
        """analyze() creates parts from LLM decomposition."""
        kp = Keeper(store_path=tmp_path)

        # First store a document
        kp.put("A long document about many topics. " * 20,
                     id="test-doc", tags={"project": "test"})

        # Mock the LLM call
        mock_response = json.dumps([
            {"summary": "Introduction", "content": "First section text",
             "tags": {"topic": "intro"}},
            {"summary": "Main body", "content": "Second section text",
             "tags": {"topic": "analysis"}},
        ])

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = [
                {"summary": "Introduction", "content": "First section text",
                 "tags": {"topic": "intro"}},
                {"summary": "Main body", "content": "Second section text",
                 "tags": {"topic": "analysis"}},
            ]
            parts = kp.analyze("test-doc")

        assert len(parts) == 2
        assert parts[0].part_num == 1
        assert parts[0].summary == "Introduction"
        assert parts[0].tags.get("topic") == "intro"
        # Parent tags inherited
        assert parts[0].tags.get("project") == "test"

    def test_analyze_replaces_parts(self, mock_providers, tmp_path):
        """Re-analyze replaces all previous parts."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Content " * 50, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            # First analysis
            mock_llm.return_value = [
                {"summary": "Part A", "content": "A text"},
                {"summary": "Part B", "content": "B text"},
                {"summary": "Part C", "content": "C text"},
            ]
            parts1 = kp.analyze("test-doc")
            assert len(parts1) == 3

            # Re-analysis with different decomposition (force=True since content unchanged)
            mock_llm.return_value = [
                {"summary": "New Part 1", "content": "New text 1"},
                {"summary": "New Part 2", "content": "New text 2"},
            ]
            parts2 = kp.analyze("test-doc", force=True)
            assert len(parts2) == 2

        # Verify only new parts exist
        listed = kp.list_parts("test-doc")
        assert len(listed) == 2
        assert listed[0].summary == "New Part 1"

    def test_get_part(self, mock_providers, tmp_path):
        """get_part() returns an Item with part metadata."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Content " * 50, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = [
                {"summary": "Part 1", "content": "Text 1"},
                {"summary": "Part 2", "content": "Text 2"},
            ]
            kp.analyze("test-doc")

        item = kp.get_part("test-doc", 1)
        assert item is not None
        assert item.summary == "Text 1"  # Returns content, not short summary
        assert item.tags["_part_num"] == "1"
        assert item.tags["_base_id"] == "test-doc"
        assert item.tags["_total_parts"] == "2"

        # Non-existent part
        assert kp.get_part("test-doc", 99) is None

    def test_list_parts(self, mock_providers, tmp_path):
        """list_parts() returns PartInfo ordered by part_num."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Content " * 50, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = [
                {"summary": f"Part {i}", "content": f"Text {i}"}
                for i in range(1, 4)
            ]
            kp.analyze("test-doc")

        parts = kp.list_parts("test-doc")
        assert len(parts) == 3
        assert [p.part_num for p in parts] == [1, 2, 3]

    def test_analyze_nonexistent_raises(self, mock_providers, tmp_path):
        """analyze() raises ValueError for nonexistent document."""
        kp = Keeper(store_path=tmp_path)
        with pytest.raises(ValueError, match="not found"):
            kp.analyze("nonexistent")


# ---------------------------------------------------------------------------
# analyze() skip / _analyzed_hash tests
# ---------------------------------------------------------------------------


class TestAnalyzeSkip:
    """Test _analyzed_hash skip logic in analyze() and enqueue_analyze()."""

    MOCK_PARTS = [
        {"summary": "Part A", "content": "Text A", "tags": {"topic": "a"}},
        {"summary": "Part B", "content": "Text B", "tags": {"topic": "b"}},
    ]

    def test_analyze_sets_analyzed_hash(self, mock_providers, tmp_path):
        """analyze() sets _analyzed_hash tag on the document after success."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

        item = kp.get("test-doc")
        assert "_analyzed_hash" in item.tags
        # Hash should match the document's content_hash
        doc_coll = kp._resolve_doc_collection()
        doc = kp._document_store.get(doc_coll, "test-doc")
        assert item.tags["_analyzed_hash"] == doc.content_hash

    def test_analyze_skips_when_current(self, mock_providers, tmp_path):
        """analyze() skips LLM call when parts are already current."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            parts1 = kp.analyze("test-doc")
            assert len(parts1) == 2

            # Second call should skip (returns existing parts, no LLM call)
            mock_llm.reset_mock()
            parts2 = kp.analyze("test-doc")
            assert len(parts2) == 2
            mock_llm.assert_not_called()

    def test_analyze_reruns_after_content_change(self, mock_providers, tmp_path):
        """analyze() re-runs when content changes after previous analysis."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Original content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

            # Change content
            kp.put("Completely different content. " * 20, id="test-doc")

            # Should re-analyze (content_hash changed)
            mock_llm.reset_mock()
            mock_llm.return_value = [
                {"summary": "New A", "content": "New text A"},
                {"summary": "New B", "content": "New text B"},
            ]
            parts = kp.analyze("test-doc")
            mock_llm.assert_called_once()
            assert parts[0].summary == "New A"

    def test_analyze_force_overrides_skip(self, mock_providers, tmp_path):
        """analyze(force=True) re-analyzes even when parts are current."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

            # Force re-analysis
            mock_llm.reset_mock()
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc", force=True)
            mock_llm.assert_called_once()

    def test_enqueue_skips_when_current(self, mock_providers, tmp_path):
        """enqueue_analyze() returns False when parts are current."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

        # Enqueue should return False (already analyzed)
        result = kp.enqueue_analyze("test-doc")
        assert result is False

    def test_enqueue_accepts_when_stale(self, mock_providers, tmp_path):
        """enqueue_analyze() returns True when content changed."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

        # Change content
        kp.put("Different content entirely. " * 20, id="test-doc")

        # Enqueue should return True (needs re-analysis)
        result = kp.enqueue_analyze("test-doc")
        assert result is True

    def test_enqueue_force_overrides(self, mock_providers, tmp_path):
        """enqueue_analyze(force=True) enqueues even when current."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

        # Force enqueue
        result = kp.enqueue_analyze("test-doc", force=True)
        assert result is True

    def test_enqueue_accepts_never_analyzed(self, mock_providers, tmp_path):
        """enqueue_analyze() returns True for never-analyzed documents."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Long content for analysis. " * 20, id="test-doc")

        result = kp.enqueue_analyze("test-doc")
        assert result is True


# ---------------------------------------------------------------------------
# Part-to-parent uplift in find()
# ---------------------------------------------------------------------------


class TestFindPartUplift:
    """find() replaces part hits with their parent document."""

    MOCK_PARTS = [
        {"summary": "Trip to Miami", "content": "Went to Miami in January",
         "tags": {"topic": "travel"}},
        {"summary": "Planning next trip", "content": "Looking at flights to NYC",
         "tags": {"topic": "travel"}},
        {"summary": "Work update", "content": "Finished the project report",
         "tags": {"topic": "work"}},
    ]

    def test_find_uplifts_part_to_parent(self, mock_providers, tmp_path):
        """When find() hits a part, it returns the parent with _focus_part."""
        kp = Keeper(store_path=tmp_path)
        kp.put("A multi-topic document about trips and work. " * 20,
               id="test-doc", tags={"project": "journal"})

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

        # Search for something a part matches
        results = kp.find("Miami trip")

        # Should find the parent, not the part
        parent_results = [r for r in results if r.id == "test-doc"]
        assert len(parent_results) >= 1
        parent = parent_results[0]
        # Should have _focus_part set
        assert "_focus_part" in parent.tags

        # Should NOT have raw part IDs in results
        part_results = [r for r in results if "@p" in r.id or "@P" in r.id]
        assert len(part_results) == 0

    def test_find_dedupes_multiple_part_hits(self, mock_providers, tmp_path):
        """Multiple parts of the same parent produce one result."""
        kp = Keeper(store_path=tmp_path)
        kp.put("Travel travel travel. " * 20,
               id="test-doc", tags={"project": "journal"})

        with patch("keep.analyzers.SlidingWindowAnalyzer.analyze") as mock_llm:
            mock_llm.return_value = list(self.MOCK_PARTS)
            kp.analyze("test-doc")

        # "travel" matches multiple parts — should still get one parent
        results = kp.find("travel")
        parent_count = sum(1 for r in results if r.id == "test-doc")
        assert parent_count <= 1


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestCLIParts:
    """Test CLI @P{N} parsing and analyze command."""

    def test_part_suffix_pattern(self):
        """PART_SUFFIX_PATTERN matches @P{N} correctly."""
        from keep.cli import PART_SUFFIX_PATTERN
        assert PART_SUFFIX_PATTERN.search("doc:1@P{1}").group(1) == "1"
        assert PART_SUFFIX_PATTERN.search("doc:1@P{42}").group(1) == "42"
        assert PART_SUFFIX_PATTERN.search("doc:1") is None
        assert PART_SUFFIX_PATTERN.search("doc:1@V{1}") is None

    def test_format_summary_line_with_part(self):
        """Summary line shows @P{N} for parts."""
        from keep.cli import _format_summary_line
        from keep.types import Item

        item = Item(
            id="doc:1@p1",
            summary="Part summary text",
            tags={"_base_id": "doc:1", "_part_num": "1", "_updated": "2026-01-14T10:00:00"},
        )
        line = _format_summary_line(item)
        assert "@P{1}" in line
        assert "Part summary" in line


# ---------------------------------------------------------------------------
# File stat fast-path tests
# ---------------------------------------------------------------------------


class TestFileStatFastPath:
    """Test stat-based fast path for file:// URI puts."""

    def test_stores_file_stat_tags(self, mock_providers, tmp_path):
        """put(uri=file://) stores _file_mtime_ns and _file_size tags."""
        kp = Keeper(store_path=tmp_path)
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, test content for stat tags.")
        file_uri = f"file://{test_file}"

        item = kp.put(uri=file_uri)
        assert item.changed is True
        assert "_file_mtime_ns" in item.tags
        assert "_file_size" in item.tags
        assert item.tags["_file_size"] == str(test_file.stat().st_size)

    def test_skips_read_when_stat_unchanged(self, mock_providers, tmp_path):
        """put() skips file read when stat (mtime+size) is unchanged."""
        kp = Keeper(store_path=tmp_path)
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, test content for stat fast path.")
        file_uri = f"file://{test_file}"

        kp.put(uri=file_uri)

        # Second put — stat fast path should skip the file read
        with patch.object(
            kp._document_provider, "fetch",
            wraps=kp._document_provider.fetch,
        ) as mock_fetch:
            item2 = kp.put(uri=file_uri)
            assert item2.changed is False
            mock_fetch.assert_not_called()

    def test_reads_file_when_stat_changes(self, mock_providers, tmp_path):
        """put() calls fetch() when file stat changes (fast path not used)."""
        import time
        kp = Keeper(store_path=tmp_path)
        test_file = tmp_path / "test.txt"
        test_file.write_text("Original content.")
        file_uri = f"file://{test_file}"

        kp.put(uri=file_uri)

        # Modify file (sleep to ensure mtime_ns changes)
        time.sleep(0.01)
        test_file.write_text("Modified content, now different!")

        # Verify fetch IS called (stat changed → fast path falls through)
        with patch.object(
            kp._document_provider, "fetch",
            wraps=kp._document_provider.fetch,
        ) as mock_fetch:
            kp.put(uri=file_uri)
            mock_fetch.assert_called_once()

    def test_falls_through_when_tags_differ(self, mock_providers, tmp_path):
        """put() reads file when user tags differ, even if stat unchanged."""
        kp = Keeper(store_path=tmp_path)
        test_file = tmp_path / "test.txt"
        test_file.write_text("Stable content.")
        file_uri = f"file://{test_file}"

        kp.put(uri=file_uri, tags={"project": "alpha"})

        # Same file, different tags — must not use fast path
        with patch.object(
            kp._document_provider, "fetch",
            wraps=kp._document_provider.fetch,
        ) as mock_fetch:
            kp.put(uri=file_uri, tags={"project": "beta"})
            mock_fetch.assert_called_once()

    def test_fast_path_with_same_tags(self, mock_providers, tmp_path):
        """put() uses fast path when user tags are the same as stored."""
        kp = Keeper(store_path=tmp_path)
        test_file = tmp_path / "test.txt"
        test_file.write_text("Content with consistent tags.")
        file_uri = f"file://{test_file}"

        kp.put(uri=file_uri, tags={"project": "myproject"})

        # Same file, same tags — should use fast path
        with patch.object(
            kp._document_provider, "fetch",
            wraps=kp._document_provider.fetch,
        ) as mock_fetch:
            item2 = kp.put(uri=file_uri, tags={"project": "myproject"})
            assert item2.changed is False
            mock_fetch.assert_not_called()
