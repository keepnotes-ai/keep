"""Tests for URI normalization (RFC 3986 §6.2.2)."""

import pytest

from keep.types import (
    MAX_TAG_VALUES_PER_KEY,
    validate_id,
    normalize_id,
    normalize_tag_map,
    tag_values,
    _normalize_http_uri,
    _decode_unreserved,
    _resolve_dot_segments,
)


# ---------------------------------------------------------------------------
# Unit tests: _decode_unreserved
# ---------------------------------------------------------------------------


class TestDecodeUnreserved:
    def test_no_encoding(self):
        assert _decode_unreserved("hello-world") == "hello-world"

    def test_decode_letters(self):
        assert _decode_unreserved("%41%42%43") == "ABC"

    def test_decode_digit(self):
        assert _decode_unreserved("%30") == "0"

    def test_decode_hyphen(self):
        assert _decode_unreserved("%2D") == "-"

    def test_decode_tilde(self):
        assert _decode_unreserved("%7E") == "~"

    def test_decode_dot(self):
        assert _decode_unreserved("%2E") == "."

    def test_decode_underscore(self):
        assert _decode_unreserved("%5F") == "_"

    def test_preserve_reserved_slash(self):
        assert _decode_unreserved("%2F") == "%2F"

    def test_preserve_reserved_equals(self):
        assert _decode_unreserved("%3D") == "%3D"

    def test_preserve_reserved_ampersand(self):
        assert _decode_unreserved("%26") == "%26"

    def test_uppercase_hex_for_reserved(self):
        assert _decode_unreserved("%2f") == "%2F"
        assert _decode_unreserved("%3d") == "%3D"

    def test_mixed(self):
        assert _decode_unreserved("hello%20%41world") == "hello%20Aworld"

    def test_no_percent(self):
        assert _decode_unreserved("plain") == "plain"

    def test_truncated_percent(self):
        assert _decode_unreserved("ab%4") == "ab%4"

    def test_invalid_hex(self):
        assert _decode_unreserved("%GG") == "%GG"


# ---------------------------------------------------------------------------
# Unit tests: _resolve_dot_segments
# ---------------------------------------------------------------------------


class TestResolveDotSegments:
    def test_no_dots(self):
        assert _resolve_dot_segments("/a/b/c") == "/a/b/c"

    def test_single_dot(self):
        assert _resolve_dot_segments("/a/./b") == "/a/b"

    def test_double_dot(self):
        assert _resolve_dot_segments("/a/b/../c") == "/a/c"

    def test_multiple_double_dots(self):
        assert _resolve_dot_segments("/a/b/c/../../d") == "/a/d"

    def test_leading_double_dot(self):
        assert _resolve_dot_segments("/../a") == "/a"

    def test_root_only(self):
        assert _resolve_dot_segments("/") == "/"

    def test_empty_path(self):
        assert _resolve_dot_segments("") == ""


# ---------------------------------------------------------------------------
# Unit tests: _normalize_http_uri
# ---------------------------------------------------------------------------


class TestNormalizeHttpUri:
    # Scheme lowercasing
    def test_scheme_lower(self):
        assert _normalize_http_uri("HTTPS://example.com/path") == "https://example.com/path"

    def test_scheme_mixed(self):
        assert _normalize_http_uri("HtTpS://example.com/path") == "https://example.com/path"

    def test_http_scheme(self):
        assert _normalize_http_uri("HTTP://example.com/path") == "http://example.com/path"

    # Host lowercasing
    def test_host_lower(self):
        assert _normalize_http_uri("https://Example.COM/path") == "https://example.com/path"

    def test_host_www(self):
        assert _normalize_http_uri("https://WWW.EXAMPLE.COM/path") == "https://www.example.com/path"

    # Default port removal
    def test_https_443(self):
        assert _normalize_http_uri("https://example.com:443/path") == "https://example.com/path"

    def test_http_80(self):
        assert _normalize_http_uri("http://example.com:80/path") == "http://example.com/path"

    def test_nondefault_port_preserved(self):
        assert _normalize_http_uri("https://example.com:8443/path") == "https://example.com:8443/path"

    def test_http_nondefault_preserved(self):
        assert _normalize_http_uri("http://example.com:8080/path") == "http://example.com:8080/path"

    # Unreserved percent-decoding in path
    def test_decode_letters_in_path(self):
        assert _normalize_http_uri("https://example.com/%41%42%43") == "https://example.com/ABC"

    def test_decode_tilde_in_path(self):
        assert _normalize_http_uri("https://example.com/%7E/page") == "https://example.com/~/page"

    def test_reserved_stays_encoded(self):
        assert _normalize_http_uri("https://example.com/path%2Fmore") == "https://example.com/path%2Fmore"

    # Dot segment resolution
    def test_dot_segments(self):
        assert _normalize_http_uri("https://example.com/a/../b") == "https://example.com/b"

    def test_single_dot(self):
        assert _normalize_http_uri("https://example.com/a/./b") == "https://example.com/a/b"

    def test_complex_dots(self):
        assert _normalize_http_uri("https://example.com/a/b/c/../../d") == "https://example.com/a/d"

    # Empty path
    def test_empty_path(self):
        assert _normalize_http_uri("https://example.com") == "https://example.com/"

    # Path case preserved
    def test_path_case_preserved(self):
        assert _normalize_http_uri("https://example.com/ReadMe.MD") == "https://example.com/ReadMe.MD"

    # Query and fragment preserved
    def test_query_preserved(self):
        assert _normalize_http_uri("https://example.com/path?b=2&a=1") == "https://example.com/path?b=2&a=1"

    def test_fragment_preserved(self):
        assert _normalize_http_uri("https://example.com/path#Section") == "https://example.com/path#Section"

    # Combined
    def test_combined(self):
        assert _normalize_http_uri("HTTPS://Example.COM:443/a/../b/%41?q=1") == "https://example.com/b/A?q=1"

    # IP address host
    def test_ip_host_port_removal(self):
        assert _normalize_http_uri("https://192.168.1.1:443/path") == "https://192.168.1.1/path"

    # Double slashes in path are significant
    def test_double_slashes_preserved(self):
        assert _normalize_http_uri("https://example.com//a//b") == "https://example.com//a//b"


# ---------------------------------------------------------------------------
# Unit tests: normalize_id
# ---------------------------------------------------------------------------


class TestNormalizeId:
    # Non-URI IDs pass through unchanged
    def test_plain_id(self):
        assert normalize_id("my-note-123") == "my-note-123"

    def test_mem_id(self):
        assert normalize_id("mem:2026-01-15T10:30:00") == "mem:2026-01-15T10:30:00"

    def test_now(self):
        assert normalize_id("now") == "now"

    def test_system_id(self):
        assert normalize_id(".tag/act") == ".tag/act"

    def test_file_uri_passthrough(self):
        assert normalize_id("file:///Users/hugh/doc.md") == "file:///Users/hugh/doc.md"

    # HTTP URIs get normalized
    def test_http_normalized(self):
        assert normalize_id("HTTPS://Example.COM/path") == "https://example.com/path"

    def test_http_port_normalized(self):
        assert normalize_id("https://example.com:443/path") == "https://example.com/path"

    # Idempotent
    def test_idempotent(self):
        result = normalize_id("HTTPS://Example.COM:443/a/../b")
        assert normalize_id(result) == result

    # Validation still works
    def test_empty_raises(self):
        with pytest.raises(ValueError):
            normalize_id("")

    def test_too_long_raises(self):
        with pytest.raises(ValueError):
            normalize_id("a" * 1025)

    def test_blocked_chars_raise(self):
        with pytest.raises(ValueError):
            normalize_id("bad<id>")

    def test_leading_whitespace_rejected(self):
        with pytest.raises(ValueError, match="leading or trailing whitespace"):
            normalize_id(" note")

    def test_trailing_whitespace_rejected(self):
        with pytest.raises(ValueError, match="leading or trailing whitespace"):
            normalize_id("note ")

    def test_nfc_normalization_applied(self):
        # "Cafe" + combining acute accent on e
        decomposed = "Cafe\u0301"
        assert normalize_id(decomposed) == "Café"


class TestValidateId:
    def test_blocked_char_check_runs_on_nfc_form(self, monkeypatch):
        import re
        import keep.types as types

        # Contrived matcher so we can observe normalization-before-check.
        monkeypatch.setattr(types, "_ID_BLOCKED_RE", re.compile("é"))
        with pytest.raises(ValueError, match="invalid characters"):
            validate_id("Cafe\u0301")


# ---------------------------------------------------------------------------
# Integration tests with Keeper
# ---------------------------------------------------------------------------


class TestNormalizationIntegration:
    """Test that URI normalization works end-to-end through Keeper."""

    @pytest.fixture
    def keeper(self, mock_providers, tmp_path):
        from keep.api import Keeper
        return Keeper(str(tmp_path / "test_store"))

    def test_put_get_normalized(self, keeper):
        """put() with non-canonical URI, get() with canonical form."""
        item = keeper.put(content="test content", id="https://Example.COM/page")
        assert item.id == "https://example.com/page"
        found = keeper.get("https://example.com/page")
        assert found is not None
        assert found.id == "https://example.com/page"

    def test_put_get_variant(self, keeper):
        """get() with variant casing finds normalized item."""
        keeper.put(content="test", id="https://example.com/page")
        found = keeper.get("HTTPS://EXAMPLE.COM/page")
        assert found is not None

    def test_duplicate_upsert(self, keeper):
        """Two puts with equivalent URIs update the same item."""
        item1 = keeper.put(content="version 1", id="https://Example.COM/doc")
        item2 = keeper.put(content="version 2", id="HTTPS://example.com/doc")
        assert item1.id == item2.id

    def test_exists_variant(self, keeper):
        """exists() finds items via variant URI."""
        keeper.put(content="test", id="https://example.com/page")
        assert keeper.exists("HTTPS://Example.COM/page")

    def test_delete_variant(self, keeper):
        """delete() with variant URI works."""
        keeper.put(content="test", id="https://example.com/page")
        assert keeper.delete("HTTPS://Example.COM/page")
        assert not keeper.exists("https://example.com/page")

    def test_tag_variant(self, keeper):
        """tag() with variant URI updates the right item."""
        keeper.put(content="test", id="https://example.com/page")
        result = keeper.tag("HTTPS://Example.COM/page", tags={"topic": "test"})
        assert result is not None
        assert result.tags.get("topic") == "test"

    def test_port_normalization(self, keeper):
        """Default port is removed during normalization."""
        item = keeper.put(content="test", id="https://example.com:443/path")
        assert item.id == "https://example.com/path"
        assert keeper.exists("https://example.com/path")

    def test_non_uri_unchanged(self, keeper):
        """Non-URI IDs are not affected."""
        item = keeper.put(content="test", id="my-note")
        assert item.id == "my-note"

    def test_put_rejects_id_with_surrounding_whitespace(self, keeper):
        with pytest.raises(ValueError, match="leading or trailing whitespace"):
            keeper.put(content="test", id=" my-note ")


class TestTagValueUriNormalization:
    """HTTP/HTTPS tag values are URI-normalized like IDs."""

    def test_tag_values_fold_http_equivalents(self):
        tags = normalize_tag_map({
            "ref": [
                "HTTPS://Example.COM:443/a/../b/%41?q=1",
                "https://example.com/b/A?q=1",
            ],
        })
        assert tag_values(tags, "ref") == ["https://example.com/b/A?q=1"]

    def test_non_http_values_unchanged(self):
        tags = normalize_tag_map({"ref": ["file:///A/B", "file:///A/B"]})
        # Non-HTTP schemes do not get URI folding.
        assert tag_values(tags, "ref") == ["file:///A/B"]

    def test_values_trim_surrounding_whitespace(self):
        tags = normalize_tag_map({"topic": ["  hello world  ", "hello world"]})
        assert tag_values(tags, "topic") == ["hello world"]

    def test_values_preserve_internal_whitespace(self):
        tags = normalize_tag_map({"topic": ["hello   world"]})
        assert tag_values(tags, "topic") == ["hello   world"]

    def test_uri_values_trim_then_normalize(self):
        tags = normalize_tag_map(
            {"ref": ["  HTTPS://Example.COM:443/a/../b/%41?q=1  "]}
        )
        assert tag_values(tags, "ref") == ["https://example.com/b/A?q=1"]

    def test_values_nfc_normalized_and_deduped(self):
        tags = normalize_tag_map({"speaker": ["Cafe\u0301", "Café"]})
        assert tag_values(tags, "speaker") == ["Café"]

    def test_max_tag_values_per_key_allows_limit(self):
        values = [f"v{i}" for i in range(MAX_TAG_VALUES_PER_KEY)]
        tags = normalize_tag_map({"topic": values})
        assert len(tag_values(tags, "topic")) == MAX_TAG_VALUES_PER_KEY

    def test_max_tag_values_per_key_rejects_overflow(self):
        values = [f"v{i}" for i in range(MAX_TAG_VALUES_PER_KEY + 1)]
        with pytest.raises(ValueError, match="Too many distinct values"):
            normalize_tag_map({"topic": values})

    def test_max_tag_values_per_key_counts_distinct_only(self):
        values = ["x"] * (MAX_TAG_VALUES_PER_KEY + 100)
        tags = normalize_tag_map({"topic": values})
        assert tag_values(tags, "topic") == ["x"]
