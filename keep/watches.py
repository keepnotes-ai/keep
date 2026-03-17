"""Daemon-driven source monitoring (files, directories, URLs).

Watch entries are stored in `.watches` system docs. The daemon polls
watched sources on its tick and re-puts anything that has changed.
Override docs like `.watches/PT5M` group entries by interval.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from .api import Keeper

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DOC_COLLECTION = "default"
_WATCHES_ID = ".watches"
_WATCHES_PREFIX = ".watches/"
_DEFAULT_INTERVAL = "PT30S"

# ---------------------------------------------------------------------------
# ISO 8601 duration parsing (subset: PnD, PTnH, PTnM, PTnS and combos)
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(
    r"^P(?:(\d+)D)?"
    r"(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$",
    re.IGNORECASE,
)


def parse_duration(iso: str) -> timedelta:
    """Parse an ISO 8601 duration string to a timedelta.

    Supports: P7D, PT30S, PT5M, PT1H, P1DT12H, etc.
    """
    m = _DURATION_RE.match(iso.strip())
    if not m:
        raise ValueError(f"Invalid ISO 8601 duration: {iso!r}")
    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)
    td = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    if td.total_seconds() == 0:
        raise ValueError(f"Duration must be positive: {iso!r}")
    return td


# ---------------------------------------------------------------------------
# WatchEntry
# ---------------------------------------------------------------------------

@dataclass
class WatchEntry:
    """A single watched source."""

    source: str             # file:// URI, directory path, or https:// URL
    kind: str               # "file" | "directory" | "url"
    added_at: str = ""      # ISO 8601 timestamp
    last_checked: str = ""  # ISO 8601 timestamp
    last_changed: str = ""  # ISO 8601 timestamp
    interval: str = _DEFAULT_INTERVAL  # ISO 8601 duration
    recurse: bool = False
    exclude: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    stale: bool = False
    # URL-specific
    etag: str = ""
    last_modified: str = ""
    # Directory-specific
    walk_hash: str = ""
    # File-specific
    mtime_ns: str = ""
    file_size: str = ""

    def is_due(self, now: datetime | None = None) -> bool:
        """Return True if this entry is due for a check."""
        if not self.last_checked:
            return True
        now = now or datetime.now(timezone.utc)
        try:
            last = datetime.fromisoformat(self.last_checked)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            return now >= last + parse_duration(self.interval)
        except (ValueError, TypeError):
            return True


def _entry_to_dict(entry: WatchEntry) -> dict:
    """Serialize a WatchEntry to a dict, omitting empty defaults."""
    d = asdict(entry)
    # Drop empty/false/default values for compact YAML
    return {k: v for k, v in d.items() if v and v != _DEFAULT_INTERVAL} | (
        {"interval": entry.interval} if entry.interval != _DEFAULT_INTERVAL else {}
    )


def _dict_to_entry(d: dict) -> WatchEntry:
    """Deserialize a dict to a WatchEntry."""
    # Handle unknown keys gracefully
    known = {f.name for f in WatchEntry.__dataclass_fields__.values()}
    filtered = {k: v for k, v in d.items() if k in known}
    return WatchEntry(**filtered)


# ---------------------------------------------------------------------------
# Storage: load / save watches from `.watches` system docs
# ---------------------------------------------------------------------------

def _interval_for_doc_id(doc_id: str) -> str:
    """Extract the interval from a watches doc ID.

    .watches -> PT30S (default)
    .watches/PT5M -> PT5M
    """
    if doc_id == _WATCHES_ID:
        return _DEFAULT_INTERVAL
    suffix = doc_id[len(_WATCHES_PREFIX):]
    # Validate it parses
    parse_duration(suffix)
    return suffix


def load_watches(keeper: Keeper) -> list[WatchEntry]:
    """Load all watch entries from `.watches` and `.watches/*` docs."""
    entries: list[WatchEntry] = []
    ds = keeper._document_store

    # Load the base .watches doc
    rec = ds.get(_DOC_COLLECTION, _WATCHES_ID)
    if rec and rec.summary:
        try:
            items = yaml.safe_load(rec.summary)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        entry = _dict_to_entry(item)
                        if not entry.interval or entry.interval == _DEFAULT_INTERVAL:
                            entry.interval = _DEFAULT_INTERVAL
                        entries.append(entry)
        except yaml.YAMLError:
            logger.warning("Failed to parse .watches doc")

    # Load override docs (.watches/PT5M, .watches/P7D, etc.)
    override_recs = ds.query_by_id_prefix(_DOC_COLLECTION, _WATCHES_PREFIX)
    for orec in override_recs:
        if not orec.summary:
            continue
        try:
            interval = _interval_for_doc_id(orec.id)
        except ValueError:
            logger.warning("Invalid watches override doc ID: %s", orec.id)
            continue
        try:
            items = yaml.safe_load(orec.summary)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        entry = _dict_to_entry(item)
                        entry.interval = interval
                        entries.append(entry)
        except yaml.YAMLError:
            logger.warning("Failed to parse %s doc", orec.id)

    return entries


def save_watches(keeper: Keeper, entries: list[WatchEntry]) -> None:
    """Persist watch entries back to `.watches` and `.watches/*` docs."""
    ds = keeper._document_store
    from .types import utc_now as _utc_now
    now_ts = _utc_now()

    # Group entries by interval
    by_interval: dict[str, list[dict]] = {}
    for entry in entries:
        interval = entry.interval or _DEFAULT_INTERVAL
        by_interval.setdefault(interval, []).append(_entry_to_dict(entry))

    # Write default interval entries to .watches
    default_entries = by_interval.pop(_DEFAULT_INTERVAL, [])
    _upsert_watches_doc(ds, _WATCHES_ID, default_entries, now_ts)

    # Write override interval entries to .watches/<interval>
    for interval, group in by_interval.items():
        doc_id = f"{_WATCHES_PREFIX}{interval}"
        _upsert_watches_doc(ds, doc_id, group, now_ts)

    # Clean up override docs that no longer have entries
    existing = ds.query_by_id_prefix(_DOC_COLLECTION, _WATCHES_PREFIX)
    active_ids = {f"{_WATCHES_PREFIX}{iv}" for iv in by_interval}
    for rec in existing:
        if rec.id not in active_ids:
            _upsert_watches_doc(ds, rec.id, [], now_ts)


def _upsert_watches_doc(ds: Any, doc_id: str, entries: list[dict], now_ts: str) -> None:
    """Upsert a single watches doc with YAML content."""
    if not entries and doc_id != _WATCHES_ID:
        # Delete empty override docs
        try:
            ds.delete(_DOC_COLLECTION, doc_id)
        except Exception:
            pass
        return
    content = yaml.safe_dump(entries, default_flow_style=False) if entries else ""
    content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
    tags = {
        "category": "system",
        "_source": "inline",
        "_updated": now_ts,
    }
    ds.upsert(
        collection=_DOC_COLLECTION,
        id=doc_id,
        summary=content,
        tags=tags,
        content_hash=content_hash,
        archive=False,
    )


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_watch(
    keeper: Keeper,
    source: str,
    kind: str,
    *,
    tags: dict[str, str] | None = None,
    recurse: bool = False,
    exclude: list[str] | None = None,
    interval: str = _DEFAULT_INTERVAL,
    max_watches: int = 100,
) -> WatchEntry:
    """Add a watch entry. Raises ValueError on limit or duplicate."""
    entries = load_watches(keeper)

    # Check for duplicate
    for e in entries:
        if e.source == source:
            raise ValueError(f"Already watching: {source}")

    if len(entries) >= max_watches:
        raise ValueError(
            f"Watch limit reached ({max_watches}). "
            "Remove a watch or increase max_watches in keep.toml."
        )

    # Validate interval
    parse_duration(interval)

    from .types import utc_now as _utc_now
    now = _utc_now()

    entry = WatchEntry(
        source=source,
        kind=kind,
        added_at=now,
        interval=interval,
        recurse=recurse,
        exclude=exclude or [],
        tags=tags or {},
    )

    # Compute initial fingerprint
    if kind == "file":
        _update_file_fingerprint(entry)
    elif kind == "directory":
        _update_directory_fingerprint(entry)
    # URLs: fingerprint is empty until first poll

    entries.append(entry)
    save_watches(keeper, entries)
    return entry


def remove_watch(keeper: Keeper, source: str) -> bool:
    """Remove a watch by source. Returns True if found."""
    entries = load_watches(keeper)
    before = len(entries)
    entries = [e for e in entries if e.source != source]
    if len(entries) == before:
        return False
    save_watches(keeper, entries)
    return True


def list_watches(keeper: Keeper) -> list[WatchEntry]:
    """List all watch entries."""
    return load_watches(keeper)


def has_active_watches(keeper: Keeper) -> bool:
    """Return True if there are any non-stale watches."""
    entries = load_watches(keeper)
    return any(not e.stale for e in entries)


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

def _update_file_fingerprint(entry: WatchEntry) -> None:
    """Update a file entry's mtime_ns and file_size from disk."""
    try:
        uri = entry.source
        path = Path(uri.removeprefix("file://")) if uri.startswith("file://") else Path(uri)
        st = path.stat()
        entry.mtime_ns = str(st.st_mtime_ns)
        entry.file_size = str(st.st_size)
        entry.stale = False
    except OSError:
        entry.stale = True


def check_file(entry: WatchEntry) -> bool:
    """Check if a watched file has changed. Returns True if changed."""
    uri = entry.source
    path = Path(uri.removeprefix("file://")) if uri.startswith("file://") else Path(uri)
    try:
        st = path.stat()
    except OSError:
        entry.stale = True
        return False

    new_mtime = str(st.st_mtime_ns)
    new_size = str(st.st_size)
    if new_mtime != entry.mtime_ns or new_size != entry.file_size:
        entry.mtime_ns = new_mtime
        entry.file_size = new_size
        entry.stale = False
        return True
    return False


def _compute_walk_hash(directory: Path, recurse: bool, exclude: list[str] | None) -> str:
    """Compute a hash of sorted (relpath, mtime_ns) for a directory."""
    from .utils import _list_directory_files
    files = _list_directory_files(directory, recurse=recurse, exclude=exclude)
    parts = []
    for f in files:
        try:
            relpath = str(f.relative_to(directory))
            mtime_ns = str(f.stat().st_mtime_ns)
            parts.append(f"{relpath}:{mtime_ns}")
        except OSError:
            continue
    combined = "\n".join(sorted(parts))
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _update_directory_fingerprint(entry: WatchEntry) -> None:
    """Update a directory entry's walk_hash from disk."""
    try:
        directory = Path(entry.source)
        entry.walk_hash = _compute_walk_hash(directory, entry.recurse, entry.exclude or None)
        entry.stale = False
    except OSError:
        entry.stale = True


def check_directory(entry: WatchEntry) -> bool:
    """Check if a watched directory has changed. Returns True if changed."""
    directory = Path(entry.source)
    if not directory.is_dir():
        entry.stale = True
        return False

    new_hash = _compute_walk_hash(directory, entry.recurse, entry.exclude or None)
    if new_hash != entry.walk_hash:
        entry.walk_hash = new_hash
        entry.stale = False
        return True
    return False


def check_url(entry: WatchEntry) -> bool:
    """Check if a watched URL has changed. Returns True if changed."""
    try:
        from .providers.http import http_session
        session = http_session()
    except Exception:
        logger.warning("HTTP session unavailable for URL watch: %s", entry.source)
        return False

    headers: dict[str, str] = {}
    if entry.etag:
        headers["If-None-Match"] = entry.etag
    if entry.last_modified:
        headers["If-Modified-Since"] = entry.last_modified

    try:
        resp = session.get(entry.source, headers=headers, timeout=30)
    except Exception as e:
        logger.warning("URL watch fetch failed for %s: %s", entry.source, e)
        return False

    if resp.status_code == 304:
        return False
    if resp.status_code == 404:
        entry.stale = True
        return False
    if resp.status_code >= 400:
        logger.warning("URL watch got %d for %s", resp.status_code, entry.source)
        return False

    # 200 or other 2xx — check if content actually changed
    new_etag = resp.headers.get("ETag", "")
    new_last_modified = resp.headers.get("Last-Modified", "")

    # If server provides cache headers, trust them
    if new_etag or new_last_modified:
        entry.etag = new_etag
        entry.last_modified = new_last_modified
        entry.stale = False
        return True

    # No cache headers — fall back to content hash comparison
    content_hash = hashlib.sha256(resp.content).hexdigest()[:16]
    if content_hash == entry.walk_hash:
        # Reuse walk_hash field for URL content hash
        return False
    entry.walk_hash = content_hash
    entry.stale = False
    return True


# ---------------------------------------------------------------------------
# Poll: the daemon calls this on each tick
# ---------------------------------------------------------------------------

def poll_watches(keeper: Keeper) -> dict[str, int]:
    """Check all due watches and re-put changed sources.

    Returns: {checked, changed, stale, errors}
    """
    entries = load_watches(keeper)
    if not entries:
        return {"checked": 0, "changed": 0, "stale": 0, "errors": 0}

    now = datetime.now(timezone.utc)
    from .types import utc_now as _utc_now
    now_ts = _utc_now()

    stats = {"checked": 0, "changed": 0, "stale": 0, "errors": 0}
    dirty = False

    for entry in entries:
        if entry.stale:
            stats["stale"] += 1
            continue
        if not entry.is_due(now):
            continue

        stats["checked"] += 1
        changed = False

        try:
            if entry.kind == "file":
                changed = check_file(entry)
            elif entry.kind == "directory":
                changed = check_directory(entry)
            elif entry.kind == "url":
                changed = check_url(entry)
        except Exception as e:
            logger.warning("Watch check error for %s: %s", entry.source, e)
            stats["errors"] += 1

        entry.last_checked = now_ts
        dirty = True

        if changed:
            stats["changed"] += 1
            entry.last_changed = now_ts
            try:
                _reput_source(keeper, entry)
            except Exception as e:
                logger.warning("Watch re-put failed for %s: %s", entry.source, e)
                stats["errors"] += 1

        if entry.stale:
            stats["stale"] += 1

    if dirty:
        save_watches(keeper, entries)

    return stats


def _reput_source(keeper: Keeper, entry: WatchEntry) -> None:
    """Re-put a watched source into the store."""
    tags = entry.tags or None
    if entry.kind == "file":
        keeper.put(uri=entry.source, tags=tags, force=True)
    elif entry.kind == "directory":
        # Walk the directory but let keep's mtime fast-path skip unchanged
        # files. The walk-hash told us *something* changed; individual file
        # mtimes narrow the blast radius without force=True.
        from .utils import _list_directory_files
        directory = Path(entry.source)
        files = _list_directory_files(
            directory, recurse=entry.recurse, exclude=entry.exclude or None,
        )
        for fpath in files:
            file_uri = f"file://{fpath}"
            try:
                keeper.put(uri=file_uri, tags=tags)
            except Exception as e:
                logger.warning("Watch re-put failed for %s: %s", file_uri, e)
    elif entry.kind == "url":
        keeper.put(uri=entry.source, tags=tags, force=True)


# ---------------------------------------------------------------------------
# Scheduling helpers
# ---------------------------------------------------------------------------

def next_check_delay(entries: list[WatchEntry]) -> float:
    """Seconds until the soonest watch entry is due.

    Returns 0.0 if any entry is overdue or has never been checked.
    """
    if not entries:
        return 30.0

    now = datetime.now(timezone.utc)
    min_delay = float("inf")

    for entry in entries:
        if entry.stale:
            continue
        if not entry.last_checked:
            return 0.0
        try:
            last = datetime.fromisoformat(entry.last_checked)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            interval = parse_duration(entry.interval or _DEFAULT_INTERVAL)
            due_at = last + interval
            delay = (due_at - now).total_seconds()
            min_delay = min(min_delay, delay)
        except (ValueError, TypeError):
            return 0.0

    return max(0.0, min_delay) if min_delay != float("inf") else 30.0
