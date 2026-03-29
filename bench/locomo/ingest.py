#!/usr/bin/env python3
# Copyright (c) 2026 Inguz Outcomes LLC.
"""LoCoMo-Plus benchmark — ingest prepared data into an isolated keep store.

Requirements (turns-as-versions strategy — the correct one for benchmarking):
  - Each session is a vstring (item ID like conv0-session1)
  - Each turn within a session is a version of that item
  - created_at is set to the parsed session date (ISO 8601)
  - Conversation-level grouping via 'conv' tag
  - Images get the session date from their conv+session tags
  - Embedding provider: Ollama with nomic-embed-text
  - Summarization: truncate (no LLM summarization during ingest)

Usage:
    python ingest.py --store stores/run-002 --strategy turns-as-versions --data-dir prepared/
    python ingest.py --store stores/run-003 --strategy per-session --data-dir prepared/
"""

import argparse
import json
import sys
import time
from pathlib import Path

from keep.api import Keeper
from keep.config import StoreConfig, ProviderConfig


def _create_store(store_path: Path) -> Keeper:
    """Create a new isolated keep store with nomic-embed-text."""
    if store_path.exists():
        print(f"ERROR: Store already exists at {store_path}", file=sys.stderr)
        print("Refusing to overwrite. Use a new run number.", file=sys.stderr)
        sys.exit(1)

    store_path.mkdir(parents=True)
    config = StoreConfig(
        path=store_path.resolve(),
        embedding=ProviderConfig("ollama", {
            "model": "nomic-embed-text",
        }),
        summarization=ProviderConfig("ollama", {"model": "llama3.2:3b"}),
        max_inline_length=25000,  # large enough for all sessions
        max_summary_length=25000,  # match for versions (summary=content for short text)
    )
    kp = Keeper(config=config)
    return kp


def ingest_session_notes(kp: Keeper, data_dir: Path) -> int:
    """Ingest per-session notes. Returns count."""
    notes = json.loads((data_dir / "session_notes.json").read_text())
    count = 0
    for note in notes:
        kp.put(note["content"], id=note["id"], tags=note["tags"])
        count += 1
        if count % 50 == 0:
            print(f"  sessions: {count}/{len(notes)}", flush=True)
    return count


def ingest_turn_notes(kp: Keeper, data_dir: Path) -> int:
    """Ingest per-turn notes. Returns count."""
    notes = json.loads((data_dir / "turn_notes.json").read_text())
    count = 0
    for note in notes:
        kp.put(note["content"], id=note["id"], tags=note["tags"])
        count += 1
        if count % 500 == 0:
            print(f"  turns: {count}/{len(notes)}", flush=True)
    return count


def _parse_locomo_date(date_str: str) -> str | None:
    """Parse LoCoMo date like '1:56 pm on 8 May, 2023' to ISO 8601.

    Returns None if unparseable (caller should decide how to handle).
    """
    from datetime import datetime
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%I:%M %p on %d %B, %Y").isoformat()
    except ValueError:
        print(f"  WARN: unparseable date '{date_str}', using None", flush=True)
        return None


def _build_session_date_lookup(data_dir: Path) -> dict[str, str]:
    """Build conv+session → ISO date lookup from versioned_session_notes.

    Used to assign timestamps to image notes that lack their own date.
    Returns dict like {"0:1": "2023-05-08T13:56:00", ...}.
    """
    sessions = json.loads((data_dir / "versioned_session_notes.json").read_text())
    lookup = {}
    for s in sessions:
        key = f"{s['tags']['conv']}:{s['tags']['session']}"
        parsed = _parse_locomo_date(s["tags"].get("date", ""))
        if parsed:
            lookup[key] = parsed
    return lookup


def ingest_versioned_sessions(kp: Keeper, data_dir: Path) -> tuple[int, int]:
    """Ingest turns-as-versions. Returns (doc_count, version_count).

    Each session becomes a vstring (item ID). Each turn becomes a version.
    The session date is set as created_at on the first version; subsequent
    versions inherit the same timestamp (turns within a session share a date).
    Conversation-level grouping is via the 'conv' tag.
    """
    sessions = json.loads((data_dir / "versioned_session_notes.json").read_text())
    doc_count = 0
    version_count = 0
    for session in sessions:
        doc_id = session["id"]
        base_tags = session["tags"]
        # Parse session date into ISO 8601 for created_at
        session_date = _parse_locomo_date(base_tags.get("date", ""))
        for vi, version in enumerate(session["versions"]):
            # Merge session tags with per-turn tags
            merged_tags = {**base_tags, **version["tags"]}
            kp.put(
                version["content"],
                id=doc_id,
                tags=merged_tags,
                created_at=session_date,
                force=(vi > 0),
            )
            version_count += 1
        doc_count += 1
        if doc_count % 50 == 0:
            print(f"  versioned sessions: {doc_count}/{len(sessions)} ({version_count} versions)", flush=True)
    return doc_count, version_count


def _sanitize_image_id(url: str) -> str:
    """Sanitize image URL for use as keep ID.

    Keep IDs cannot contain certain characters. We hash long/problematic URLs
    but keep short clean ones readable. The original URL is stored as a tag.
    """
    import hashlib
    import re
    # If URL is clean enough for an ID, use it directly
    if len(url) < 200 and re.match(r'^https?://[\w./-]+\.\w+$', url):
        return url
    # Otherwise hash it, prefix with img: for readability
    h = hashlib.sha256(url.encode()).hexdigest()[:16]
    return f"img:{h}"


def ingest_image_notes(kp: Keeper, data_dir: Path,
                       session_dates: dict[str, str] | None = None) -> int:
    """Ingest image notes. Returns count.

    session_dates: optional conv:session → ISO date lookup for created_at.
    """
    notes = json.loads((data_dir / "image_notes.json").read_text())
    count = 0
    skipped = 0
    for note in notes:
        img_id = _sanitize_image_id(note["id"])
        tags = {**note["tags"], "_source_url": note["id"]}
        # Look up session date for this image
        created_at = None
        if session_dates:
            key = f"{tags.get('conv', '')}:{tags.get('session', '')}"
            created_at = session_dates.get(key)
        try:
            kp.put(note["content"], id=img_id, tags=tags, created_at=created_at)
            count += 1
        except (ValueError, Exception) as e:
            print(f"  WARN: skipping image {note['id'][:60]}: {e}", flush=True)
            skipped += 1
        if (count + skipped) % 200 == 0:
            print(f"  images: {count}/{len(notes)} ({skipped} skipped)", flush=True)
    if skipped:
        print(f"  images: {skipped} skipped due to errors")
    return count


def write_state(store_path: Path, state: dict):
    """Write STATE.json to the store directory."""
    state_path = store_path / "STATE.json"
    with open(state_path, "w") as f:
        json.dump(state, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Ingest LoCoMo-Plus data into keep store")
    parser.add_argument("--store", type=str, required=True,
                        help="Path for new isolated store")
    parser.add_argument("--strategy", type=str, required=True,
                        choices=["per-session", "per-turn", "turns-as-versions"],
                        help="Ingestion strategy")
    parser.add_argument("--data-dir", type=str, default="prepared",
                        help="Path to prepared dataset (default: prepared/)")
    parser.add_argument("--skip-images", action="store_true",
                        help="Skip image note ingestion")
    args = parser.parse_args()

    store_path = Path(args.store).resolve()
    data_dir = Path(args.data_dir).resolve()

    if not data_dir.exists():
        print(f"ERROR: Data dir not found: {data_dir}", file=sys.stderr)
        sys.exit(1)

    # Pre-flight: verify Ollama is running and model is available
    import urllib.request
    try:
        req = urllib.request.Request(
            "http://localhost:11434/api/embeddings",
            data=json.dumps({"model": "nomic-embed-text", "prompt": "test"}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            dim = len(result["embedding"])
            print(f"Pre-flight OK: Ollama nomic-embed-text → {dim}d embeddings")
    except Exception as e:
        print(f"ERROR: Ollama pre-flight failed: {e}", file=sys.stderr)
        print("Ensure Ollama is running with nomic-embed-text pulled.", file=sys.stderr)
        sys.exit(1)

    print(f"Creating store at {store_path}")
    print(f"Strategy: {args.strategy}")
    kp = _create_store(store_path)

    state = {
        "store_path": str(store_path),
        "strategy": args.strategy,
        "keep_version": None,
        "embedding_model": "nomic-embed-text (ollama)",
        "phases_completed": [],
    }

    t0 = time.time()

    # Ingest content
    if args.strategy == "per-session":
        count = ingest_session_notes(kp, data_dir)
        print(f"Ingested {count} session notes")
    elif args.strategy == "per-turn":
        count = ingest_turn_notes(kp, data_dir)
        print(f"Ingested {count} turn notes")
    elif args.strategy == "turns-as-versions":
        docs, versions = ingest_versioned_sessions(kp, data_dir)
        print(f"Ingested {docs} session docs ({versions} versions)")
        count = versions

    # Images
    img_count = 0
    if not args.skip_images:
        session_dates = _build_session_date_lookup(data_dir)
        img_count = ingest_image_notes(kp, data_dir, session_dates=session_dates)
        print(f"Ingested {img_count} image notes")

    elapsed = time.time() - t0

    state["phases_completed"].append({
        "phase": "ingest",
        "completed": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "strategy": args.strategy,
        "content_notes": count,
        "image_notes": img_count,
        "elapsed_seconds": round(elapsed, 1),
    })
    write_state(store_path, state)

    print(f"\nIngestion complete in {elapsed:.1f}s")
    print(f"STATE.json written to {store_path}/STATE.json")


if __name__ == "__main__":
    main()
