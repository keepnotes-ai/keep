"""Tests for the watches module (daemon-driven source monitoring)."""

from datetime import timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from keep.watches import (
    WatchEntry,
    parse_duration,
    load_watches,
    save_watches,
    add_watch,
    remove_watch,
    list_watches,
    has_active_watches,
    check_file,
    check_directory,
    check_url,
    poll_watches,
    next_check_delay,
    _compute_walk_hash,
)


# ---------------------------------------------------------------------------
# parse_duration
# ---------------------------------------------------------------------------

class TestParseDuration:

    def test_seconds(self):
        assert parse_duration("PT30S") == timedelta(seconds=30)

    def test_minutes(self):
        assert parse_duration("PT5M") == timedelta(minutes=5)

    def test_hours(self):
        assert parse_duration("PT1H") == timedelta(hours=1)

    def test_days(self):
        assert parse_duration("P7D") == timedelta(days=7)

    def test_combined(self):
        assert parse_duration("P1DT12H") == timedelta(days=1, hours=12)

    def test_case_insensitive(self):
        assert parse_duration("pt30s") == timedelta(seconds=30)

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_duration("not-a-duration")

    def test_zero(self):
        with pytest.raises(ValueError):
            parse_duration("PT0S")


# ---------------------------------------------------------------------------
# WatchEntry.is_due
# ---------------------------------------------------------------------------

class TestWatchEntryIsDue:

    def test_never_checked(self):
        entry = WatchEntry(source="x", kind="file")
        assert entry.is_due()

    def test_not_yet_due(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        entry = WatchEntry(
            source="x", kind="file",
            last_checked=now.isoformat(),
            interval="PT30S",
        )
        assert not entry.is_due(now)

    def test_past_due(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        past = (now - timedelta(minutes=1)).isoformat()
        entry = WatchEntry(
            source="x", kind="file",
            last_checked=past,
            interval="PT30S",
        )
        assert entry.is_due(now)


# ---------------------------------------------------------------------------
# CRUD (requires Keeper with document store)
# ---------------------------------------------------------------------------

class TestWatchCRUD:

    @pytest.fixture
    def kp(self, mock_providers, tmp_path):
        from keep.api import Keeper
        return Keeper(store_path=tmp_path)

    def test_empty_store(self, kp):
        assert list_watches(kp) == []

    def test_add_and_list(self, kp):
        entry = add_watch(kp, "file:///tmp/test.txt", "file")
        watches = list_watches(kp)
        assert len(watches) == 1
        assert watches[0].source == "file:///tmp/test.txt"
        assert watches[0].kind == "file"

    def test_add_duplicate(self, kp):
        add_watch(kp, "file:///tmp/test.txt", "file")
        with pytest.raises(ValueError, match="Already watching"):
            add_watch(kp, "file:///tmp/test.txt", "file")

    def test_add_max_limit(self, kp):
        for i in range(3):
            add_watch(kp, f"file:///tmp/f{i}.txt", "file", max_watches=3)
        with pytest.raises(ValueError, match="Watch limit"):
            add_watch(kp, "file:///tmp/extra.txt", "file", max_watches=3)

    def test_remove(self, kp):
        add_watch(kp, "file:///tmp/test.txt", "file")
        assert remove_watch(kp, "file:///tmp/test.txt") is True
        assert list_watches(kp) == []

    def test_remove_nonexistent(self, kp):
        assert remove_watch(kp, "file:///tmp/nope.txt") is False

    def test_has_active_watches(self, kp, tmp_path):
        assert not has_active_watches(kp)
        f = tmp_path / "test.txt"
        f.write_text("hello")
        add_watch(kp, f"file://{f}", "file")
        assert has_active_watches(kp)

    def test_add_with_tags(self, kp):
        add_watch(kp, "file:///tmp/test.txt", "file", tags={"project": "docs"})
        watches = list_watches(kp)
        assert watches[0].tags == {"project": "docs"}

    def test_add_directory_with_recurse_and_exclude(self, kp, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        add_watch(kp, str(d), "directory", recurse=True, exclude=["*.log"])
        watches = list_watches(kp)
        assert watches[0].recurse is True
        assert watches[0].exclude == ["*.log"]

    def test_override_interval(self, kp):
        add_watch(kp, "https://example.com", "url", interval="PT5M")
        watches = list_watches(kp)
        assert len(watches) == 1
        assert watches[0].interval == "PT5M"

    def test_mixed_intervals(self, kp):
        add_watch(kp, "file:///tmp/a.txt", "file")  # default PT30S
        add_watch(kp, "https://example.com", "url", interval="PT5M")
        watches = list_watches(kp)
        assert len(watches) == 2
        intervals = {w.source: w.interval for w in watches}
        assert intervals["file:///tmp/a.txt"] == "PT30S"
        assert intervals["https://example.com"] == "PT5M"


# ---------------------------------------------------------------------------
# Change detection: files
# ---------------------------------------------------------------------------

class TestCheckFile:

    def test_unchanged(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        st = f.stat()
        entry = WatchEntry(
            source=f"file://{f}",
            kind="file",
            mtime_ns=str(st.st_mtime_ns),
            file_size=str(st.st_size),
        )
        assert check_file(entry) is False

    def test_changed(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello")
        st = f.stat()
        entry = WatchEntry(
            source=f"file://{f}",
            kind="file",
            mtime_ns=str(st.st_mtime_ns),
            file_size=str(st.st_size),
        )
        f.write_text("hello world")
        assert check_file(entry) is True
        # Fingerprint updated
        assert entry.mtime_ns != str(st.st_mtime_ns)

    def test_stale(self, tmp_path):
        entry = WatchEntry(
            source=f"file://{tmp_path / 'gone.txt'}",
            kind="file",
            mtime_ns="12345",
        )
        assert check_file(entry) is False
        assert entry.stale is True


# ---------------------------------------------------------------------------
# Change detection: directories
# ---------------------------------------------------------------------------

class TestCheckDirectory:

    def test_unchanged(self, tmp_path):
        (tmp_path / "a.txt").write_text("A")
        (tmp_path / "b.txt").write_text("B")
        walk_hash = _compute_walk_hash(tmp_path, recurse=False, exclude=None)
        entry = WatchEntry(
            source=str(tmp_path),
            kind="directory",
            walk_hash=walk_hash,
        )
        assert check_directory(entry) is False

    def test_changed_new_file(self, tmp_path):
        (tmp_path / "a.txt").write_text("A")
        walk_hash = _compute_walk_hash(tmp_path, recurse=False, exclude=None)
        entry = WatchEntry(
            source=str(tmp_path),
            kind="directory",
            walk_hash=walk_hash,
        )
        (tmp_path / "b.txt").write_text("B")
        assert check_directory(entry) is True

    def test_stale_dir_gone(self, tmp_path):
        d = tmp_path / "subdir"
        d.mkdir()
        entry = WatchEntry(source=str(d), kind="directory", walk_hash="old")
        d.rmdir()
        assert check_directory(entry) is False
        assert entry.stale is True


# ---------------------------------------------------------------------------
# Change detection: URLs
# ---------------------------------------------------------------------------

class TestCheckURL:

    def test_304_not_modified(self):
        entry = WatchEntry(
            source="https://example.com/doc",
            kind="url",
            etag='"abc123"',
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 304
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        with patch("keep.providers.http.http_session", return_value=mock_session):
            assert check_url(entry) is False

    def test_200_changed(self):
        entry = WatchEntry(
            source="https://example.com/doc",
            kind="url",
            etag='"abc123"',
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"ETag": '"def456"', "Last-Modified": "Mon, 17 Mar 2026"}
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        with patch("keep.providers.http.http_session", return_value=mock_session):
            assert check_url(entry) is True
            assert entry.etag == '"def456"'

    def test_404_stale(self):
        entry = WatchEntry(source="https://example.com/gone", kind="url")
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        with patch("keep.providers.http.http_session", return_value=mock_session):
            assert check_url(entry) is False
            assert entry.stale is True


# ---------------------------------------------------------------------------
# poll_watches integration
# ---------------------------------------------------------------------------

class TestPollWatches:

    @pytest.fixture
    def kp(self, mock_providers, tmp_path):
        from keep.api import Keeper
        return Keeper(store_path=tmp_path)

    def test_poll_empty(self, kp):
        result = poll_watches(kp)
        assert result == {"checked": 0, "changed": 0, "stale": 0, "errors": 0}

    def test_poll_file_changed(self, kp, tmp_path):
        f = tmp_path / "watched.txt"
        f.write_text("original")
        add_watch(kp, f"file://{f}", "file")

        # Modify the file
        f.write_text("updated content")

        # Force the entry to be due (clear last_checked)
        entries = load_watches(kp)
        entries[0].last_checked = ""
        save_watches(kp, entries)

        result = poll_watches(kp)
        assert result["checked"] == 1
        assert result["changed"] == 1

    def test_poll_file_unchanged(self, kp, tmp_path):
        f = tmp_path / "watched.txt"
        f.write_text("stable")
        add_watch(kp, f"file://{f}", "file")

        # Force the entry to be due
        entries = load_watches(kp)
        entries[0].last_checked = ""
        save_watches(kp, entries)

        result = poll_watches(kp)
        assert result["checked"] == 1
        assert result["changed"] == 0

    def test_poll_stale_file(self, kp, tmp_path):
        f = tmp_path / "ephemeral.txt"
        f.write_text("here now")
        add_watch(kp, f"file://{f}", "file")
        f.unlink()

        entries = load_watches(kp)
        entries[0].last_checked = ""
        save_watches(kp, entries)

        result = poll_watches(kp)
        assert result["stale"] >= 1


# ---------------------------------------------------------------------------
# next_check_delay
# ---------------------------------------------------------------------------

class TestNextCheckDelay:

    def test_empty(self):
        assert next_check_delay([]) == 30.0

    def test_never_checked(self):
        entry = WatchEntry(source="x", kind="file")
        assert next_check_delay([entry]) == 0.0

    def test_future(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        entry = WatchEntry(
            source="x", kind="file",
            last_checked=now.isoformat(),
            interval="PT30S",
        )
        delay = next_check_delay([entry])
        assert 0 < delay <= 30.0
