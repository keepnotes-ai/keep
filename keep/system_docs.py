"""System document management — constants, loading, and migration.

System documents are bundled .md files that provide reference material
(tag specs, meta-doc definitions, etc.) for keep stores. They're loaded
on first use and upgraded when the bundled content changes.
"""

import hashlib
import importlib.resources
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .api import Keeper

logger = logging.getLogger(__name__)


def _get_system_doc_dir() -> Path:
    """Get path to system docs, works in both dev and installed environments.

    Tries in order:
    1. Package data via importlib.resources (installed packages)
    2. Relative path inside package (development)
    3. Legacy path outside package (backwards compatibility)
    """
    try:
        with importlib.resources.as_file(
            importlib.resources.files("keep.data.system")
        ) as path:
            if path.exists():
                return path
    except (ModuleNotFoundError, TypeError):
        pass

    dev_path = Path(__file__).parent / "data" / "system"
    if dev_path.exists():
        return dev_path

    return Path(__file__).parent.parent / "docs" / "system"


# Path to system documents
SYSTEM_DOC_DIR = _get_system_doc_dir()

# Stable IDs for system documents — derived from filename.
# Convention: strip .md, replace leading hyphens with / according to prefix
# depth, keep remaining hyphens literal, prefix with a dot.
#
# Examples:
#   tag-act-commitment.md                    -> .tag/act/commitment
#   prompt-agent-session-start.md            -> .prompt/agent/session-start
_PREFIX_DEPTH = {
    "tag": 2,
    "meta": 1,
    "prompt": 2,
    "state": 1,
}


def _filename_to_id(filename: str) -> str:
    """Derive a stable document ID from a system doc filename."""
    stem = filename.removesuffix(".md")
    parts = stem.split("-")
    depth = _PREFIX_DEPTH.get(parts[0], 0)
    hierarchy = parts[:depth + 1]
    remainder = parts[depth + 1:]
    out = "/".join(hierarchy)
    if remainder:
        out += "-" + "-".join(remainder)
    return "." + out


def _all_system_doc_ids() -> dict[str, str]:
    """Build mapping of relative paths to stable IDs for all system docs.

    Scans top-level .md files and also subdirectories (for state doc
    fragments).  Subdirectory files get IDs derived from the parent
    dir name + child filename, e.g.::

        state-after-write/ocr.md  ->  .state/after-write/ocr
    """
    result: dict[str, str] = {}
    for p in sorted(SYSTEM_DOC_DIR.glob("*.md")):
        if p.name != "__init__.py":
            result[p.name] = _filename_to_id(p.name)
    # Subdirectories: state doc fragment files
    for d in sorted(SYSTEM_DOC_DIR.iterdir()):
        if d.is_dir() and not d.name.startswith(("_", ".")):
            parent_id = _filename_to_id(d.name + ".md")
            for p in sorted(d.glob("*.md")):
                rel_key = f"{d.name}/{p.name}"
                result[rel_key] = f"{parent_id}/{p.stem}"
    return result


SYSTEM_DOC_IDS = _all_system_doc_ids()

# Migration renames from old ID prefixes to new stable IDs
_OLD_ID_RENAMES = {
    "_system:now": ".now",
    "_system:conversations": ".conversations",
    "_system:domains": ".domains",
    "_system:library": ".library",
    "_tag:act": ".tag/act",
    "_tag:status": ".tag/status",
    "_tag:project": ".tag/project",
    "_tag:topic": ".tag/topic",
    "_now:default": "now",
}


def _load_frontmatter(path: Path) -> tuple[str, dict[str, str]]:
    """Load content and tags from a file with optional YAML frontmatter.

    Returns:
        (content, tags) tuple. Tags empty if no frontmatter.

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    text = path.read_text()

    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            import yaml
            frontmatter = yaml.safe_load(parts[1])
            content = parts[2].lstrip("\n")
            if frontmatter:
                tags = frontmatter.get("tags", {})
                tags = {k: str(v) for k, v in tags.items()}
                return content, tags
            return content, {}

    return text, {}


def _content_hash(content: str) -> str:
    """Short SHA256 hash of content for change detection."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[-10:]


def _bundled_docs_hash() -> str:
    """Composite hash of all bundled system doc files.

    Computed from sorted filenames + content so any change to any
    bundled doc produces a different hash.  Includes subdirectory
    fragment files.  Used as a gate to skip the per-file migration
    scan when nothing has changed.
    """
    h = hashlib.sha256()
    # Use SYSTEM_DOC_IDS keys (sorted rel paths) to ensure stable ordering
    for rel_path in sorted(SYSTEM_DOC_IDS.keys()):
        path = SYSTEM_DOC_DIR / rel_path
        if path.exists():
            h.update(rel_path.encode("utf-8"))
            h.update(path.read_bytes())
    return h.hexdigest()[-10:]


def migrate_system_documents(keeper: "Keeper", progress=None) -> dict:
    """Migrate system documents to stable IDs and current version.

    Handles:
    - Migration from old file:// URIs to stable IDs
    - Rename of old prefixes (_system:, _tag:, _now:, _text:) to new (.x, .tag/x, now, %x)
    - Fresh creation for new stores
    - Version upgrades when bundled content changes

    Called during init. Only loads docs that don't already exist,
    so user modifications are preserved. Updates config version
    after successful migration.

    Args:
        keeper: Keeper instance
        progress: Optional callback(current, total, label) for progress reporting

    Returns:
        Dict with migration stats: created, migrated, skipped, cleaned
    """
    from .config import save_config
    from .types import casefold_tags_for_index

    stats = {"created": 0, "migrated": 0, "skipped": 0, "cleaned": 0}

    current_hash = _bundled_docs_hash()
    if keeper._config.system_docs_hash == current_hash:
        return stats

    filename_to_id = _all_system_doc_ids()

    # First pass: clean up old file:// URIs with category=system tag
    try:
        old_system_docs = keeper.list_items(tags={"category": "system"}, limit=100)
        for doc in old_system_docs:
            if doc.id.startswith("file://") and doc.id.endswith(".md"):
                filename = Path(doc.id.replace("file://", "")).name
                new_id = filename_to_id.get(filename)
                if new_id and not keeper.exists(new_id):
                    keeper.put(doc.summary, id=new_id, tags=doc.tags)
                    keeper.delete(doc.id)
                    stats["migrated"] += 1
                    logger.info("Migrated system doc: %s -> %s", doc.id, new_id)
                elif new_id:
                    keeper.delete(doc.id)
                    stats["cleaned"] += 1
                    logger.info("Cleaned up old system doc: %s", doc.id)
    except (OSError, ValueError, KeyError, RuntimeError) as e:
        logger.debug("Error scanning old system docs: %s", e)

    # Second pass: rename old prefixes to new
    for old_id, new_id in _OLD_ID_RENAMES.items():
        try:
            old_item = keeper.get(old_id)
            if old_item and not keeper.exists(new_id):
                keeper.put(old_item.summary, id=new_id, tags=old_item.tags)
                keeper.delete(old_id)
                stats["migrated"] += 1
                logger.info("Renamed ID: %s -> %s", old_id, new_id)
            elif old_item:
                keeper.delete(old_id)
                stats["cleaned"] += 1
        except (OSError, ValueError, KeyError, RuntimeError) as e:
            logger.debug("Error renaming %s: %s", old_id, e)

    # Rename _text:hash -> %hash (transfer embeddings directly, no re-embedding)
    # Preserves original timestamps - these are user memories with meaningful dates
    try:
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll_name = keeper._resolve_chroma_collection()
        old_text_docs = keeper._document_store.query_by_id_prefix(doc_coll, "_text:")
        for rec in old_text_docs:
            new_id = "%" + rec.id[len("_text:"):]
            if not keeper._document_store.get(doc_coll, new_id):
                keeper._document_store.copy_record(doc_coll, rec.id, new_id)
                try:
                    entries = keeper._store.get_entries_full(chroma_coll_name, [rec.id])
                    if entries and entries[0].get("embedding") is not None:
                        entry = entries[0]
                        keeper._store.upsert_batch(
                            chroma_coll_name,
                            [new_id],
                            [entry["embedding"]],
                            [entry["summary"] or rec.summary],
                            [casefold_tags_for_index(entry["tags"])],
                        )
                except (ValueError, KeyError) as e:
                    logger.debug("ChromaDB transfer skipped for %s: %s", rec.id, e)
            keeper.delete(rec.id)
            stats["migrated"] += 1
            logger.info("Renamed text ID: %s -> %s", rec.id, new_id)
    except (OSError, ValueError, KeyError) as e:
        logger.debug("Error migrating _text: IDs: %s", e)

    # Third pass: remove system docs no longer bundled
    _RETIRED_SYSTEM_IDS = [".meta/decisions"]
    for old_id in _RETIRED_SYSTEM_IDS:
        try:
            if keeper.exists(old_id):
                keeper.delete(old_id)
                stats["cleaned"] += 1
                logger.info("Removed retired system doc: %s", old_id)
        except (OSError, ValueError, KeyError) as e:
            logger.debug("Error removing retired doc %s: %s", old_id, e)

    # Fourth pass: create or update system docs from bundled content
    # Iterates over SYSTEM_DOC_IDS to include both top-level docs and
    # subdirectory fragment files.
    sorted_items = sorted(filename_to_id.items())
    total_items = len(sorted_items)
    for idx, (rel_path, new_id) in enumerate(sorted_items, 1):
        if progress:
            progress(idx, total_items, new_id)
        path = SYSTEM_DOC_DIR / rel_path
        if not path.exists():
            continue

        try:
            content, tags = _load_frontmatter(path)
            bundled_hash = _content_hash(content)
            tags["category"] = "system"
            tags["bundled_hash"] = bundled_hash

            # Check existing doc: skip if unchanged, update base if user edited
            existing_doc = keeper._document_store.get(doc_coll, new_id)
            if existing_doc:
                prev_hash = existing_doc.tags.get("bundled_hash")
                if isinstance(prev_hash, list):
                    prev_hash = prev_hash[0] if prev_hash else None
                if prev_hash == bundled_hash:
                    # Content unchanged — skip to avoid creating spurious versions
                    continue
                if prev_hash and existing_doc.content_hash != prev_hash:
                    # User has modified the doc — update the archived base
                    # version so reverting restores the latest bundled content.
                    base_ver = keeper._document_store.find_version_by_content_hash(
                        doc_coll, new_id, prev_hash,
                    )
                    if base_ver is not None:
                        keeper._document_store.replace_version_content(
                            doc_coll, new_id, base_ver,
                            summary=content, tags=tags,
                            content_hash=bundled_hash,
                        )
                    # Update bundled_hash on head so next upgrade knows current base
                    keeper._document_store.patch_head_tags(
                        doc_coll, new_id, {"bundled_hash": bundled_hash},
                    )
                    stats["migrated"] += 1
                    logger.info("Updated base version of user-edited system doc: %s", new_id)
                    continue

            # Store to DocumentStore directly (always works, no embedding needed).
            # System docs are reference material - store full verbatim content.
            # Use archive=False to update in-place without creating spurious
            # version history (the old bundled content is not worth keeping).
            from .types import utc_now as _utc_now
            now_ts = _utc_now()
            tags.setdefault("_created", now_ts)
            tags["_updated"] = now_ts
            tags["_updated_date"] = now_ts[:10]
            tags["_source"] = "inline"
            keeper._document_store.upsert(
                collection=doc_coll, id=new_id, summary=content,
                tags=tags, content_hash=bundled_hash,
                archive=False,
            )
            # All bundled system docs have dot-prefix IDs — no embedding
            # or background processing needed (looked up by ID, not search).

            # Activate edge backfill for tagdocs with _inverse
            if new_id.startswith(".tag/") and "/" not in new_id[5:]:
                old_inverse = existing_doc.tags.get("_inverse") if existing_doc else None
                if isinstance(old_inverse, list):
                    old_inverse = old_inverse[0]
                new_inverse = tags.get("_inverse")
                if isinstance(new_inverse, list):
                    new_inverse = new_inverse[0]
                if new_inverse and new_inverse != old_inverse:
                    # New or changed _inverse → enqueue backfill
                    if old_inverse:
                        keeper._document_store.delete_edges_for_predicate(doc_coll, new_id[5:])
                        keeper._document_store.delete_backfill(doc_coll, new_id[5:])
                    keeper._check_edge_backfill(new_id[5:], new_inverse, doc_coll)
                    # Materialize the inverse tagdoc synchronously
                    keeper._ensure_inverse_tagdoc(new_id[5:], new_inverse, doc_coll)
                elif old_inverse and not new_inverse:
                    # _inverse removed → clean up
                    keeper._document_store.delete_edges_for_predicate(doc_coll, new_id[5:])
                    keeper._document_store.delete_backfill(doc_coll, new_id[5:])

            if existing_doc:
                stats["migrated"] += 1
                logger.info("Updated system doc: %s", new_id)
            else:
                stats["created"] += 1
                logger.info("Created system doc: %s", new_id)
        except FileNotFoundError:
            pass

    keeper._config.system_docs_hash = current_hash
    save_config(keeper._config)

    n_enqueued = stats["created"] + stats["migrated"]
    if n_enqueued > 0:
        logger.info(
            "System docs migration: %d created, %d updated, %d skipped. "
            "Enqueued %d for background embedding.",
            stats["created"], stats["migrated"], stats["skipped"], n_enqueued,
        )

    return stats


def reset_system_documents(keeper: "Keeper") -> dict:
    """Reset all system documents to bundled content.

    Deletes any user override versions and restores the head to
    the current bundled content.  The document's creation timestamp
    is preserved; only version history is cleared.

    Returns:
        Dict with stats: reset count, versions_deleted count
    """
    from .config import save_config
    from .types import utc_now as _utc_now

    stats = {"reset": 0, "versions_deleted": 0}
    doc_coll = keeper._resolve_doc_collection()

    for path in SYSTEM_DOC_DIR.glob("*.md"):
        if path.name == "__init__.py":
            continue
        new_id = _filename_to_id(path.name)

        try:
            content, tags = _load_frontmatter(path)
            bundled_hash = _content_hash(content)
            tags["category"] = "system"
            tags["bundled_hash"] = bundled_hash

            now_ts = _utc_now()
            tags.setdefault("_created", now_ts)
            tags["_updated"] = now_ts
            tags["_updated_date"] = now_ts[:10]
            tags["_source"] = "inline"

            # Delete all archived versions (user overrides)
            n_deleted = keeper._document_store.delete_all_versions(
                doc_coll, new_id,
            )
            stats["versions_deleted"] += n_deleted

            # Update head in-place with fresh bundled content (no archiving)
            keeper._document_store.upsert(
                collection=doc_coll, id=new_id, summary=content,
                tags=tags, content_hash=bundled_hash,
                archive=False,
            )
            stats["reset"] += 1
            logger.info("Reset system doc: %s (removed %d versions)", new_id, n_deleted)

        except FileNotFoundError:
            logger.warning("System doc file not found: %s", path)

    keeper._config.system_docs_hash = _bundled_docs_hash()
    save_config(keeper._config)

    return stats
