"""
Test harness for keep.

Sample documents in docs/library/:
- an5.57: Upajjhāyasutta (Five Remembrances) - English JSON
- fortytwo_chapters: 佛說四十二章經 (Sutra in 42 Sections) - Chinese text
- ancrenewisse: Ancrene Wisse (Anchoresses' Guide) - PDF
- mumford_sticks_and_stones: Lewis Mumford on American architecture - English text

These represent diverse content: languages, formats, traditions.
Test data files are discovered dynamically to support adding more.
"""

import pytest
from pathlib import Path

from keep import (
    Item,
    filter_non_system_tags,
    SYSTEM_TAG_PREFIX,
)
from keep.types import Item


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def data_dir() -> Path:
    """Path to test data directory."""
    return Path(__file__).parent.parent / "docs" / "library"


@pytest.fixture
def an5_57_path(data_dir: Path) -> Path:
    """Upajjhāyasutta (Five Remembrances) - English JSON."""
    return data_dir / "an5.57_translation-en-sujato.json"


@pytest.fixture
def fortytwo_chapters_path(data_dir: Path) -> Path:
    """Sutra in 42 Sections - Chinese text."""
    return data_dir / "fortytwo_chapters.txt"


@pytest.fixture
def mumford_path(data_dir: Path) -> Path:
    """Sticks and Stones - English text."""
    return data_dir / "mumford_sticks_and_stones.txt"


@pytest.fixture
def ancrenewisse_path(data_dir: Path) -> Path:
    """Ancrene Wisse - PDF."""
    return data_dir / "ancrenewisse.pdf"


# -----------------------------------------------------------------------------
# Item Tests
# -----------------------------------------------------------------------------

class TestItem:
    """Tests for Item dataclass."""
    
    def test_item_creation_minimal(self):
        """Item can be created with just id and summary."""
        item = Item(id="test:1", summary="A test item")
        assert item.id == "test:1"
        assert item.summary == "A test item"
        assert item.tags == {}
        assert item.score is None
    
    def test_item_timestamps_from_tags(self):
        """Timestamps are accessed via properties from tags."""
        item = Item(
            id="test:2",
            summary="Test",
            tags={
                "_created": "2026-01-30T10:00:00Z",
                "_updated": "2026-01-30T11:00:00Z",
            }
        )
        assert item.created == "2026-01-30T10:00:00Z"
        assert item.updated == "2026-01-30T11:00:00Z"
    
    def test_item_timestamps_missing(self):
        """Timestamp properties return None if tags missing."""
        item = Item(id="test:3", summary="Test")
        assert item.created is None
        assert item.updated is None
    
    def test_item_with_score(self):
        """Item can have a similarity score (search results)."""
        item = Item(id="test:4", summary="Test", score=0.95)
        assert item.score == 0.95
    
    def test_item_str(self):
        """Item string representation is readable."""
        item = Item(id="test:5", summary="A" * 100)
        s = str(item)
        assert "test:5" in s
        assert "..." in s  # Truncated


# -----------------------------------------------------------------------------
# System Tag Protection Tests
# -----------------------------------------------------------------------------

class TestSystemTagProtection:
    """Tests for system tag filtering."""
    
    def test_filter_removes_system_tags(self):
        """filter_non_system_tags removes underscore-prefixed tags."""
        tags = {
            "project": "myapp",
            "category": "docs",
            "_created": "2026-01-30T10:00:00Z",
            "_sneaky": "should be removed",
        }
        filtered = filter_non_system_tags(tags)
        
        assert "project" in filtered
        assert "category" in filtered
        assert "_created" not in filtered
        assert "_sneaky" not in filtered
    
    def test_filter_preserves_non_system_tags(self):
        """filter_non_system_tags preserves regular tags."""
        tags = {"a": "1", "b": "2", "c": "3"}
        filtered = filter_non_system_tags(tags)
        assert filtered == tags
    
    def test_filter_empty_dict(self):
        """filter_non_system_tags handles empty dict."""
        assert filter_non_system_tags({}) == {}
    
    def test_filter_all_system_tags(self):
        """filter_non_system_tags can return empty dict."""
        tags = {"_a": "1", "_b": "2"}
        assert filter_non_system_tags(tags) == {}
    
    def test_system_tag_prefix(self):
        """SYSTEM_TAG_PREFIX is underscore."""
        assert SYSTEM_TAG_PREFIX == "_"


# -----------------------------------------------------------------------------
# Test Data Validation
# -----------------------------------------------------------------------------

class TestDataFiles:
    """Verify test data files are present and readable.

    Uses dynamic discovery to ensure all files in docs/library/ are tested.
    """

    def test_all_data_files_exist(self, data_dir: Path):
        """All test data files (non-README) exist and are readable."""
        files = list(data_dir.glob("*"))
        data_files = [f for f in files if f.is_file() and f.name != "INDEX.md"]

        assert len(data_files) >= 4, f"Expected at least 4 data files, found {len(data_files)}"

        for f in data_files:
            assert f.exists(), f"File {f.name} should exist"
            assert f.stat().st_size > 0, f"File {f.name} should not be empty"

    def test_all_text_files_readable(self, data_dir: Path):
        """All .txt files are readable as UTF-8."""
        for txt_file in data_dir.glob("*.txt"):
            content = txt_file.read_text(encoding="utf-8")
            assert len(content) > 100, f"{txt_file.name} should have substantial content"

    def test_all_json_files_valid(self, data_dir: Path):
        """All .json files are valid JSON."""
        import json
        for json_file in data_dir.glob("*.json"):
            data = json.loads(json_file.read_text())
            assert isinstance(data, (dict, list)), f"{json_file.name} should parse as dict or list"

    def test_all_pdf_files_valid(self, data_dir: Path):
        """All .pdf files have valid PDF header."""
        for pdf_file in data_dir.glob("*.pdf"):
            content = pdf_file.read_bytes()
            assert content.startswith(b"%PDF"), f"{pdf_file.name} should be a valid PDF"

    # Specific content tests for known files

    def test_an5_57_content(self, an5_57_path: Path):
        """Upajjhāyasutta (Five Remembrances) contains expected content."""
        import json
        data = json.loads(an5_57_path.read_text())
        assert "an5.57:0.3" in data
        assert "Reviewing" in data["an5.57:0.3"]

    def test_fortytwo_chapters_content(self, fortytwo_chapters_path: Path):
        """Sutra of Forty-Two Chapters contains Chinese text."""
        content = fortytwo_chapters_path.read_text()
        assert "佛" in content  # Buddha
        assert "沙門" in content  # śramaṇa

    def test_ancrenewisse_is_pdf(self, ancrenewisse_path: Path):
        """Ancrene Wisse is a valid PDF."""
        content = ancrenewisse_path.read_bytes()
        assert content.startswith(b"%PDF")

    def test_mumford_content(self, mumford_path: Path):
        """Mumford text contains expected content."""
        content = mumford_path.read_text()
        assert "architecture" in content.lower()
        assert "medieval" in content.lower()


# -----------------------------------------------------------------------------
# Visibility Pattern Tests  
# -----------------------------------------------------------------------------

class TestVisibilityPatterns:
    """Tests for visibility tagging conventions."""
    
    def test_draft_visibility(self):
        """Draft items have _visibility=draft."""
        item = Item(
            id="draft:1",
            summary="Work in progress",
            tags={"_visibility": "draft", "_for": "self"}
        )
        assert item.tags["_visibility"] == "draft"
        assert item.tags["_for"] == "self"
    
    def test_shared_visibility(self):
        """Shared items have _visibility=shared."""
        item = Item(
            id="shared:1", 
            summary="Ready to share",
            tags={"_visibility": "shared", "_for": "team", "_reviewed": "true"}
        )
        assert item.tags["_visibility"] == "shared"
        assert item.tags["_reviewed"] == "true"


# -----------------------------------------------------------------------------
# Nowdoc Tests
# -----------------------------------------------------------------------------

class TestNowdoc:
    """Tests for nowdoc constants and system content."""

    def test_nowdoc_id_constant(self):
        """NOWDOC_ID is exported and has expected format."""
        from keep import NOWDOC_ID

        assert NOWDOC_ID == "now"

    def test_system_now_md_exists(self):
        """System now.md file exists in keep/data/system/ with frontmatter."""
        from keep.system_docs import _load_frontmatter, SYSTEM_DOC_DIR

        content, tags = _load_frontmatter(SYSTEM_DOC_DIR / "now.md")
        assert len(content) > 0
        assert "keep_prompt" in content
        # Frontmatter should be parsed into tags
        assert isinstance(tags, dict)
        assert len(tags) > 0  # Has at least one tag from frontmatter

    def test_load_frontmatter_missing_file(self):
        """_load_frontmatter raises FileNotFoundError for missing files."""
        from keep.system_docs import _load_frontmatter, SYSTEM_DOC_DIR
        import pytest

        with pytest.raises(FileNotFoundError):
            _load_frontmatter(SYSTEM_DOC_DIR / "nonexistent.md")


# -----------------------------------------------------------------------------
# Summarization Prompt Tests
# -----------------------------------------------------------------------------

class TestSummarizationPrompts:
    """Tests for summarization prompt construction (contamination prevention)."""

    def test_system_prompt_does_not_mention_keep(self):
        """System prompt must not prime the LLM with keep's identity."""
        from keep.providers.base import SUMMARIZATION_SYSTEM_PROMPT
        assert "keep" not in SUMMARIZATION_SYSTEM_PROMPT.lower().split()

    def test_system_prompt_has_no_product_names(self):
        """System prompt examples must not name specific products (haiku will parrot them)."""
        from keep.providers.base import SUMMARIZATION_SYSTEM_PROMPT
        # Any quoted example containing a real product name will contaminate summaries
        import re
        # Check that no quoted string in the prompt contains what looks like a product pitch
        quoted = re.findall(r'"([^"]+)"', SUMMARIZATION_SYSTEM_PROMPT)
        for q in quoted:
            # Quoted examples should be meta-instructions, not product descriptions
            assert len(q.split()) < 10, f"Quoted example too long (could contaminate): {q!r}"

    def test_build_prompt_without_context(self):
        """Without context, prompt wraps content with extraction instructions."""
        from keep.providers.base import build_summarization_prompt
        result = build_summarization_prompt("Some document text.")
        assert "Some document text." in result
        assert "ONLY facts from the text" in result

    def test_build_prompt_with_context_contains_boundary(self):
        """With context, prompt must instruct LLM to summarize only the document."""
        from keep.providers.base import build_summarization_prompt
        context = "Related topics: widgets, hardware"
        result = build_summarization_prompt("Doc about gadgets.", context=context)
        assert "Summarize only the document itself" in result or "Summarize" in result
        assert "collection about:" in result

    def test_build_prompt_with_context_includes_document(self):
        """With context, prompt still includes the actual document."""
        from keep.providers.base import build_summarization_prompt
        result = build_summarization_prompt("My doc content.", context="- ctx")
        assert "My doc content." in result

    def test_strip_summary_preamble(self):
        """strip_summary_preamble removes common LLM preambles."""
        from keep.providers.base import strip_summary_preamble
        assert strip_summary_preamble("Here is a summary: Redis is fast.") == "Redis is fast."
        assert strip_summary_preamble("This document describes a system.") == "a system."
        assert strip_summary_preamble("Redis is fast.") == "Redis is fast."

    def test_strip_summary_preamble_conversation(self):
        """strip_summary_preamble removes conversation-specific preambles."""
        from keep.providers.base import strip_summary_preamble
        assert strip_summary_preamble("This conversation discusses car repairs.") == "car repairs."
        assert strip_summary_preamble("In this conversation, the user bought a phone.") == "the user bought a phone."
        assert strip_summary_preamble("The user discusses their new laptop.") == "their new laptop."

    def test_is_conversation_detects_chat_transcript(self):
        """_is_conversation detects user/assistant turn-taking patterns."""
        from keep.providers.base import _is_conversation
        chat = (
            "User: I bought a new car yesterday.\n"
            "Assistant: That's great! What kind?\n"
            "User: A Toyota Corolla, 2023 model.\n"
            "Assistant: Nice choice. How's it driving?\n"
        )
        assert _is_conversation(chat) is True

    def test_is_conversation_rejects_document(self):
        """_is_conversation rejects normal document content."""
        from keep.providers.base import _is_conversation
        doc = (
            "Redis is an in-memory data store used as a database, cache, and message broker. "
            "It supports various data structures such as strings, hashes, lists, and sets."
        )
        assert _is_conversation(doc) is False

    def test_build_prompt_conversation_with_context(self):
        """With context, conversation content gets conversation-specific framing."""
        from keep.providers.base import build_summarization_prompt
        chat = (
            "User: I bought a car on Feb 20.\n"
            "Assistant: What kind?\n"
            "User: Toyota Corolla.\n"
            "Assistant: Nice!\n"
        )
        result = build_summarization_prompt(chat, context="vehicles, purchases")
        assert "conversation" in result.lower()
        assert "Preserve ALL specific dates" in result
        assert "vehicles, purchases" in result

    def test_build_prompt_document_with_context_unchanged(self):
        """With context, document content still gets document framing."""
        from keep.providers.base import build_summarization_prompt
        doc = "Redis is a fast in-memory database used for caching."
        result = build_summarization_prompt(doc, context="databases")
        assert "document" in result.lower()
        assert "Redis" in result

    def test_clean_for_summarization_strips_urls(self):
        """Content cleaning removes URLs."""
        from keep.providers.base import _clean_for_summarization
        text = "Visit (https://example.com/page) for details. Order #123."
        result = _clean_for_summarization(text)
        assert "https://example.com" not in result
        assert "Order #123" in result

    def test_clean_for_summarization_strips_land_ack(self):
        """Content cleaning removes land acknowledgment boilerplate."""
        from keep.providers.base import _clean_for_summarization
        text = (
            "It is with gratitude and humility that we acknowledge "
            "that this building rests on the ancestral homelands "
            "of the People. We commit to building a more inclusive "
            "and equitable space for all. More information can be found here. "
            "ORDER INFO: Order #456."
        )
        result = _clean_for_summarization(text)
        assert "ancestral" not in result
        assert "Order #456" in result

    def test_strip_summary_preamble(self):
        """Preamble stripping removes common LLM filler phrases."""
        from keep.providers.base import strip_summary_preamble
        assert strip_summary_preamble("Here is a summary: Good content.") == "Good content."
        assert strip_summary_preamble("Actual summary text.") == "Actual summary text."


# -----------------------------------------------------------------------------
# Meta-doc Parser Tests
# -----------------------------------------------------------------------------

class TestMetaDocParser:
    """Tests for _parse_meta_doc() structured line parser."""

    def test_query_lines(self):
        """Query lines with key=value pairs are parsed."""
        from keep.utils import _parse_meta_doc
        queries, ctx, prereqs = _parse_meta_doc("act=commitment status=open\ntype=learning")
        assert queries == [
            {"act": "commitment", "status": "open"},
            {"type": "learning"},
        ]
        assert ctx == []
        assert prereqs == []

    def test_context_match_keys(self):
        """Context-match lines (key=) are parsed."""
        from keep.utils import _parse_meta_doc
        queries, ctx, prereqs = _parse_meta_doc("project=\ntopic=")
        assert queries == []
        assert ctx == ["project", "topic"]
        assert prereqs == []

    def test_prose_ignored(self):
        """Prose lines are ignored by the parser."""
        from keep.utils import _parse_meta_doc
        content = """# .meta/todo — Open Loops

Open loops: unresolved commitments.

act=commitment status=open

project=
"""
        queries, ctx, prereqs = _parse_meta_doc(content)
        assert queries == [{"act": "commitment", "status": "open"}]
        assert ctx == ["project"]
        assert prereqs == []

    def test_mixed_content(self):
        """Full meta-doc with prose, queries, and context."""
        from keep.utils import _parse_meta_doc
        content = """# .meta/learnings — Experiential Priming

Past learnings and breakdowns.

type=learning
type=breakdown
type=gotcha

project=
topic=
"""
        queries, ctx, prereqs = _parse_meta_doc(content)
        assert len(queries) == 3
        assert queries[0] == {"type": "learning"}
        assert queries[1] == {"type": "breakdown"}
        assert queries[2] == {"type": "gotcha"}
        assert ctx == ["project", "topic"]
        assert prereqs == []

    def test_empty_content(self):
        """Empty content returns empty lists."""
        from keep.utils import _parse_meta_doc
        queries, ctx, prereqs = _parse_meta_doc("")
        assert queries == []
        assert ctx == []
        assert prereqs == []

    def test_markdown_headings_are_prose(self):
        """Markdown headings and formatting are treated as prose."""
        from keep.utils import _parse_meta_doc
        queries, ctx, prereqs = _parse_meta_doc("# Heading\n**bold text**\n- list item")
        assert queries == []
        assert ctx == []
        assert prereqs == []

    def test_partial_key_value_is_prose(self):
        """Lines with mixed tokens (some not key=value) are prose."""
        from keep.utils import _parse_meta_doc
        queries, ctx, prereqs = _parse_meta_doc("act=commitment and also open")
        assert queries == []
        assert ctx == []
        assert prereqs == []

    def test_prerequisite_wildcard(self):
        """Prerequisite lines (key=*) gate metadoc on tag existence."""
        from keep.utils import _parse_meta_doc
        queries, ctx, prereqs = _parse_meta_doc("genre=*\ngenre=")
        assert queries == []
        assert ctx == ["genre"]
        assert prereqs == ["genre"]

    def test_mixed_prereq_query_context(self):
        """Full metadoc with prerequisites, queries, and context."""
        from keep.utils import _parse_meta_doc
        content = """# .meta/genre — Similar genres

Items in the same genre, for media with genre tags.

genre=*
type=learning
genre=
project=
"""
        queries, ctx, prereqs = _parse_meta_doc(content)
        assert prereqs == ["genre"]
        assert queries == [{"type": "learning"}]
        assert ctx == ["genre", "project"]


# -----------------------------------------------------------------------------
# System Doc Tests
# -----------------------------------------------------------------------------

class TestSystemDocs:
    """Tests for system document infrastructure."""

    def test_all_system_doc_files_exist(self):
        """Every file referenced in SYSTEM_DOC_IDS exists on disk."""
        from keep.system_docs import SYSTEM_DOC_IDS, SYSTEM_DOC_DIR
        for filename in SYSTEM_DOC_IDS:
            path = SYSTEM_DOC_DIR / filename
            assert path.exists(), f"Missing system doc file: {filename}"

    def test_system_doc_ids_are_valid(self):
        """System doc IDs follow naming conventions."""
        from keep.system_docs import SYSTEM_DOC_IDS
        for filename, doc_id in SYSTEM_DOC_IDS.items():
            assert not doc_id.startswith("_"), f"{doc_id} uses old underscore prefix"
            assert doc_id == doc_id.strip(), f"{doc_id} has whitespace"

    def test_meta_docs_are_valid_state_docs(self):
        """Meta-docs must be valid state docs with at least one rule."""
        from keep.system_docs import SYSTEM_DOC_IDS, SYSTEM_DOC_DIR, _load_frontmatter
        from keep.state_doc import parse_state_doc
        for filename, doc_id in SYSTEM_DOC_IDS.items():
            if not doc_id.startswith(".meta/"):
                continue
            content, _ = _load_frontmatter(SYSTEM_DOC_DIR / filename)
            doc = parse_state_doc(doc_id, content)
            assert len(doc.rules) > 0, \
                f"{doc_id} has no rules — meta-doc would be inert"

    def test_tag_docs_have_structure(self):
        """.tag/* docs should have documented structure (values, characteristics, or lifecycle)."""
        from keep.system_docs import SYSTEM_DOC_IDS, SYSTEM_DOC_DIR, _load_frontmatter
        markers = ("## Values", "## Characteristics", "## Speech-act lifecycle")
        for filename, doc_id in SYSTEM_DOC_IDS.items():
            if not doc_id.startswith(".tag/"):
                continue
            # Skip sub-docs (tag values like .tag/act/commitment)
            if doc_id.count("/") > 1:
                continue
            content, _ = _load_frontmatter(SYSTEM_DOC_DIR / filename)
            assert any(m in content for m in markers), \
                f"{doc_id} missing structure documentation"

    def test_bundled_hash_is_deterministic(self):
        """_content_hash produces consistent results for same input."""
        from keep.processors import _content_hash
        h1 = _content_hash("test content")
        h2 = _content_hash("test content")
        assert h1 == h2
        assert h1 != _content_hash("different content")
