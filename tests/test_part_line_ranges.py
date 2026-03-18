"""Tests for line range extraction in URI-sourced document analysis."""

import pytest

from keep.analyzers import _extract_line_ranges, _extract_markdown_sections, _match_parts_to_sections


class TestExtractLineRanges:
    """Test the _extract_line_ranges function."""

    def test_empty_inputs(self):
        """Test behavior with empty inputs."""
        # Empty parts list
        result = _extract_line_ranges("some content", [])
        assert result == []

        # Empty content
        result = _extract_line_ranges("", [{"summary": "test"}])
        assert result == [{"summary": "test"}]

        # Both empty
        result = _extract_line_ranges("", [])
        assert result == []

    def test_markdown_with_clear_headings(self):
        """Test markdown file with clear headings gets correct line ranges."""
        content = """# Introduction
This is the introduction section.
It spans multiple lines.

## Data Analysis
This section discusses data.
More content here.

### Results
The results are presented here.
Final content.

## Conclusion
This concludes the document.
"""
        
        parts = [
            {"summary": "Introduction overview explaining the topic"},
            {"summary": "Data analysis methodology and approach"},
            {"summary": "Results showing key findings"},
            {"summary": "Conclusion summarizing outcomes"}
        ]

        result = _extract_line_ranges(content, parts)
        
        # Check that line ranges were added as string tags
        assert len(result) == 4
        for part in result:
            assert "tags" in part
            assert "_start_line" in part["tags"]
            assert "_end_line" in part["tags"]
            assert isinstance(part["tags"]["_start_line"], str)
            assert isinstance(part["tags"]["_end_line"], str)

        # Check specific ranges
        assert result[0]["tags"]["_start_line"] == "1"  # Introduction starts at line 1
        assert result[1]["tags"]["_start_line"] == "5"  # Data Analysis starts at line 5
        assert result[2]["tags"]["_start_line"] == "9"  # Results starts at line 9
        assert result[3]["tags"]["_start_line"] == "13" # Conclusion starts at line 13

    def test_mixed_heading_levels(self):
        """Test markdown with mixed heading levels still works."""
        content = """# Main Title
Content here.

### Subsection A
Some content.

## Major Section
More content.

#### Deep Subsection
Final content.
"""
        
        parts = [
            {"summary": "Main title overview"},
            {"summary": "Subsection A details"},
            {"summary": "Major section content"},
            {"summary": "Deep subsection information"}
        ]

        result = _extract_line_ranges(content, parts)
        
        assert len(result) == 4
        for part in result:
            assert "tags" in part
            assert "_start_line" in part["tags"]
            assert "_end_line" in part["tags"]

    def test_non_markdown_file(self):
        """Test non-markdown file gets full-file range."""
        content = """This is a plain text file.
It has no markdown headings.
All parts should get the full file range.
More content here.
Final line."""
        
        parts = [
            {"summary": "First part"},
            {"summary": "Second part"}
        ]

        result = _extract_line_ranges(content, parts)
        
        assert len(result) == 2
        for part in result:
            assert part["tags"]["_start_line"] == "1"
            assert part["tags"]["_end_line"] == "5"  # Total lines

    def test_part_summary_matching_heading(self):
        """Test part summary that matches heading text gets correct range."""
        content = """# User Authentication
This section covers user auth.

# Database Schema
This covers the database.

# API Endpoints
This covers the API.
"""
        
        parts = [
            {"summary": "User Authentication system implementation"},
            {"summary": "Database Schema design and structure"},
            {"summary": "API Endpoints for the service"}
        ]

        result = _extract_line_ranges(content, parts)
        
        # Should match based on heading text in summaries
        assert result[0]["tags"]["_start_line"] == "1"  # User Authentication
        assert result[1]["tags"]["_start_line"] == "4"  # Database Schema  
        assert result[2]["tags"]["_start_line"] == "7"  # API Endpoints

    def test_sequential_fallback(self):
        """Test sequential fallback when summaries don't match headings."""
        content = """# Section One
Content here.

# Section Two
More content.

# Section Three
Final content.
"""
        
        parts = [
            {"summary": "Completely different topic A"},
            {"summary": "Unrelated topic B"},
            {"summary": "Another unrelated topic C"}
        ]

        result = _extract_line_ranges(content, parts)
        
        # Should fall back to sequential assignment
        assert result[0]["tags"]["_start_line"] == "1"
        assert result[1]["tags"]["_start_line"] == "4"
        assert result[2]["tags"]["_start_line"] == "7"

    def test_more_parts_than_sections(self):
        """Test case where there are more parts than markdown sections."""
        content = """# Only Section
This is the only section in the document.
It has some content.
But we have multiple parts to assign.
"""
        
        parts = [
            {"summary": "Part one"},
            {"summary": "Part two"},
            {"summary": "Part three"}
        ]

        result = _extract_line_ranges(content, parts)
        
        # Should fall back to file-level ranges for all parts
        for part in result:
            assert part["tags"]["_start_line"] == "1"
            assert part["tags"]["_end_line"] == "4"

    def test_preserves_existing_tags(self):
        """Test that existing tags on parts are preserved."""
        content = """# Section One
Content here.
"""
        
        parts = [
            {
                "summary": "Section One overview",
                "tags": {"existing": "value", "type": "important"}
            }
        ]

        result = _extract_line_ranges(content, parts)
        
        assert len(result) == 1
        tags = result[0]["tags"]
        assert tags["existing"] == "value"
        assert tags["type"] == "important"
        assert tags["_start_line"] == "1"
        assert tags["_end_line"] == "2"

    def test_error_safety(self):
        """Test that function is safe and never raises."""
        # Invalid content that might cause regex issues
        content = "# [[[Invalid regex chars]]]\nContent"
        parts = [{"summary": "test"}]
        
        # Should not raise
        result = _extract_line_ranges(content, parts)
        assert len(result) == 1
        assert "tags" in result[0]


class TestExtractMarkdownSections:
    """Test the _extract_markdown_sections helper function."""

    def test_simple_headings(self):
        """Test extraction of simple markdown headings."""
        lines = [
            "# First Heading",
            "Content for first section.",
            "",
            "## Second Heading",
            "Content for second section.",
            "More content.",
            "",
            "### Third Heading",
            "Final content."
        ]

        sections = _extract_markdown_sections(lines)
        
        assert len(sections) == 3
        
        assert sections[0]["title"] == "First Heading"
        assert sections[0]["start_line"] == 1
        assert sections[0]["end_line"] == 3  # Up to line before next heading
        
        assert sections[1]["title"] == "Second Heading"
        assert sections[1]["start_line"] == 4
        assert sections[1]["end_line"] == 7
        
        assert sections[2]["title"] == "Third Heading"
        assert sections[2]["start_line"] == 8
        assert sections[2]["end_line"] == 9  # End of file

    def test_no_headings(self):
        """Test file with no headings returns empty list."""
        lines = [
            "This is just regular text.",
            "No headings here.",
            "More plain text."
        ]

        sections = _extract_markdown_sections(lines)
        assert sections == []

    def test_heading_variations(self):
        """Test different heading formats are recognized."""
        lines = [
            "#Heading Without Space",  # Should not match - no space after #
            "# Proper Heading",       # Should match
            "  ## Indented Heading",  # Should not match (indented)
            "##    Extra Spaces   ",  # Should match
            "######    Six Levels     ",  # Should match
            "####### Too Many",       # Should not match (7 #s)
            "# ",                     # Should not match - empty title
            "Regular text"
        ]

        sections = _extract_markdown_sections(lines)
        
        # Should only match valid headings: "# Proper Heading", "##    Extra Spaces   ", "######    Six Levels     "
        assert len(sections) == 3
        assert sections[0]["title"] == "Proper Heading"
        assert sections[1]["title"] == "Extra Spaces"
        assert sections[2]["title"] == "Six Levels"


class TestMatchPartsToSections:
    """Test the _match_parts_to_sections helper function."""

    def test_exact_title_match(self):
        """Test parts that exactly match section titles."""
        parts = [
            {"summary": "Introduction to the topic"},
            {"summary": "Methodology used in study"},
            {"summary": "Results and findings"}
        ]
        
        sections = [
            {"title": "Introduction", "start_line": 1, "end_line": 5},
            {"title": "Methodology", "start_line": 6, "end_line": 10},
            {"title": "Results", "start_line": 11, "end_line": 15}
        ]

        assignments = _match_parts_to_sections(parts, sections)
        
        # Should match based on title words in summaries
        assert assignments == [0, 1, 2]

    def test_partial_match(self):
        """Test partial matching of words."""
        parts = [
            {"summary": "Background information and context"},
            {"summary": "Analysis methodology and approach"}
        ]
        
        sections = [
            {"title": "Background", "start_line": 1, "end_line": 5},
            {"title": "Analysis", "start_line": 6, "end_line": 10}
        ]

        assignments = _match_parts_to_sections(parts, sections)
        assert assignments == [0, 1]

    def test_sequential_fallback(self):
        """Test sequential assignment when no good matches found."""
        parts = [
            {"summary": "Random topic A"},
            {"summary": "Unrelated topic B"}
        ]
        
        sections = [
            {"title": "Section One", "start_line": 1, "end_line": 5},
            {"title": "Section Two", "start_line": 6, "end_line": 10}
        ]

        assignments = _match_parts_to_sections(parts, sections)
        assert assignments == [0, 1]  # Sequential assignment

    def test_more_parts_than_sections(self):
        """Test case with more parts than sections."""
        parts = [
            {"summary": "Part A"},
            {"summary": "Part B"},
            {"summary": "Part C"}
        ]
        
        sections = [
            {"title": "Section One", "start_line": 1, "end_line": 5}
        ]

        assignments = _match_parts_to_sections(parts, sections)
        assert assignments[0] == 0  # First part gets the section
        assert assignments[1] is None  # No more sections available
        assert assignments[2] is None

# ---------------------------------------------------------------------------
# _find_best_passage tests
# ---------------------------------------------------------------------------

from keep.analyzers import _find_best_passage


class TestFindBestPassage:
    """Test keyword-based passage extraction fallback."""

    SAMPLE_MD = """# Day 12 Reflection

## What happened
Day 12 was a three-phase day.

### The MLX memory leak
The intuition to save memory by releasing one model was exactly backwards.
Metal's unified memory allocator marks freed buffers as reusable within MLX
but doesn't return virtual pages to the OS.
So load/release cycling means permanent RSS growth.

### Sliding-window analyzer
The iteration from JSON to plain text summaries was the right move.
Hugh's insight: don't ask for content, just summaries.

### GKE stabilization
The 492 worker restarts were caused by a missing egress port 3307
for cloud-sql-proxy in the network policy.
"""

    def test_finds_matching_passage(self):
        result = _find_best_passage(self.SAMPLE_MD, "MLX memory leak RSS")
        assert result is not None
        assert "MLX" in result["snippet"]

    def test_finds_specific_number(self):
        result = _find_best_passage(self.SAMPLE_MD, "492 worker restarts")
        assert result is not None
        assert "492" in result["snippet"]

    def test_snaps_to_heading(self):
        # Use a query that matches deep in a section, not near the top
        result = _find_best_passage(self.SAMPLE_MD, "egress port 3307 cloud-sql-proxy")
        assert result is not None
        # Should snap to the ### GKE heading
        assert "### GKE stabilization" in result["snippet"]

    def test_no_match_returns_none(self):
        result = _find_best_passage(self.SAMPLE_MD, "xyzzy foobar")
        assert result is None

    def test_empty_inputs(self):
        assert _find_best_passage("", "query") is None
        assert _find_best_passage("content", "") is None
        assert _find_best_passage(None, "query") is None

    def test_short_terms_ignored(self):
        # Single-char terms should be skipped
        result = _find_best_passage(self.SAMPLE_MD, "a b c")
        assert result is None

    def test_returns_one_indexed_lines(self):
        result = _find_best_passage(self.SAMPLE_MD, "three-phase day")
        assert result is not None
        assert int(result["start_line"]) >= 1
        assert int(result["end_line"]) >= int(result["start_line"])
