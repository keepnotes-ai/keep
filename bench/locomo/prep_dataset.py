#!/usr/bin/env python3
# Copyright (c) 2026 Inguz Outcomes LLC.
"""LoCoMo-Plus benchmark — dataset preparation.

Reads locomo10.json and locomo_plus.json, produces a ready-to-ingest dataset:
  - session_notes.json:  per-session markdown notes with frontmatter
  - image_notes.json:    per-image notes (URL as ID, caption+query as content)
  - qa_dataset.json:     QA pairs for query/judge (5 categories)
  - cognitive_dataset.json: cognitive samples for query/judge

Usage:
    python prep_dataset.py --data-dir /path/to/Locomo-Plus/data --out-dir prepared/
"""

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


# ── Helpers ──────────────────────────────────────────────────────────────

CATEGORY_NAMES = {
    1: "multi-hop",
    2: "temporal",
    3: "open-domain",
    4: "single-hop",
    5: "adversarial",
}


def _session_keys(conv: dict) -> list[str]:
    """Sorted session keys from a conversation dict."""
    keys = [k for k in conv if k.startswith("session_") and not k.endswith("_date_time")]
    return sorted(keys, key=lambda x: int(x.split("_")[-1]))


def _render_turn(turn: dict) -> str:
    """Render a single conversation turn as a markdown line."""
    speaker = turn.get("speaker", "?")
    text = (turn.get("text") or "").strip()
    caption = turn.get("blip_caption", "")
    img_urls = turn.get("img_url", [])

    parts = [f'{speaker} said, "{text}"']

    if caption and img_urls:
        url = img_urls[0]
        parts.append(f" and shared ![{caption}]({url})")
    elif caption:
        parts.append(f" and shared {caption}")

    return "".join(parts)


def _parse_evidence(raw_evidence: list, conv: dict) -> str:
    """Convert evidence references (D1:5) to readable text."""
    lines = []
    for ev in raw_evidence:
        for part in str(ev).split(";"):
            part = part.strip()
            if not part:
                continue
            try:
                session_id, turn_id = part.split(":")
                si = int(session_id.replace("D", ""))
                ti = int(turn_id)
                session_key = f"session_{si}"
                turns = conv.get(session_key, [])
                if 0 <= ti - 1 < len(turns):
                    turn = turns[ti - 1]
                    lines.append(_render_turn(turn))
                else:
                    lines.append(f"[{part}] [missing turn]")
            except Exception:
                lines.append(f"[{part}] [parse error]")
    return "\n".join(lines)


# ── Session Notes ────────────────────────────────────────────────────────

def build_session_notes(locomo: list) -> list[dict]:
    """Build per-session markdown notes from all 10 conversations.

    Returns list of dicts:
      {id, content, tags}
    where content includes YAML frontmatter.
    """
    notes = []

    for ci, item in enumerate(locomo):
        conv = item.get("conversation", {})
        speaker_a = conv.get("speaker_a", "A")
        speaker_b = conv.get("speaker_b", "B")

        for skey in _session_keys(conv):
            si = int(skey.split("_")[-1])
            date_str = conv.get(f"{skey}_date_time", "")
            turns = conv.get(skey, [])

            # Build body
            body_lines = [f"DATE: {date_str}", ""]
            for turn in turns:
                body_lines.append(_render_turn(turn))

            body = "\n".join(body_lines)

            # Note ID
            note_id = f"conv{ci}-session{si}"

            # Tags (will become frontmatter)
            tags = {
                "conv": str(ci),
                "session": str(si),
                "speaker_a": speaker_a,
                "speaker_b": speaker_b,
                "type": "session",
            }
            if date_str:
                tags["date"] = date_str

            notes.append({
                "id": note_id,
                "content": body,
                "tags": tags,
            })

    return notes


# ── Turn Notes ───────────────────────────────────────────────────────────

def build_turn_notes(locomo: list) -> list[dict]:
    """Build per-turn notes from all 10 conversations.

    Each conversational turn becomes its own note.
    Image turns include the caption + URL inline.

    Returns list of dicts:
      {id, content, tags}
    """
    notes = []

    for ci, item in enumerate(locomo):
        conv = item.get("conversation", {})
        speaker_a = conv.get("speaker_a", "A")
        speaker_b = conv.get("speaker_b", "B")

        for skey in _session_keys(conv):
            si = int(skey.split("_")[-1])
            date_str = conv.get(f"{skey}_date_time", "")
            turns = conv.get(skey, [])

            for ti, turn in enumerate(turns, 1):
                content = _render_turn(turn)
                dia_id = turn.get("dia_id", f"D{si}:{ti}")

                note_id = f"conv{ci}-D{si}:{ti}"

                tags = {
                    "conv": str(ci),
                    "session": str(si),
                    "turn": str(ti),
                    "speaker": turn.get("speaker", "?"),
                    "speaker_a": speaker_a,
                    "speaker_b": speaker_b,
                    "type": "turn",
                    "dia_id": dia_id,
                }
                if date_str:
                    tags["date"] = date_str

                notes.append({
                    "id": note_id,
                    "content": content,
                    "tags": tags,
                })

    return notes


# ── Versioned Session Notes (turns as versions) ─────────────────────────

def build_versioned_session_notes(locomo: list) -> list[dict]:
    """Build per-session documents where each turn is a version.

    Returns list of dicts:
      {id, tags, versions: [{content, tags}, ...]}
    Versions are ordered chronologically (turn 1 first).
    The loader should put() each version in order to the same ID.
    """
    sessions = []

    for ci, item in enumerate(locomo):
        conv = item.get("conversation", {})
        speaker_a = conv.get("speaker_a", "A")
        speaker_b = conv.get("speaker_b", "B")

        for skey in _session_keys(conv):
            si = int(skey.split("_")[-1])
            date_str = conv.get(f"{skey}_date_time", "")
            turns = conv.get(skey, [])

            note_id = f"conv{ci}-session{si}"

            # Session-level tags (applied on first put, inherited)
            session_tags = {
                "conv": str(ci),
                "session": str(si),
                "speaker_a": speaker_a,
                "speaker_b": speaker_b,
                "type": "session",
            }
            if date_str:
                session_tags["date"] = date_str

            versions = []
            for ti, turn in enumerate(turns, 1):
                content = _render_turn(turn)
                # Per-version tags (speaker of this turn)
                turn_tags = {
                    "speaker": turn.get("speaker", "?"),
                    "turn": str(ti),
                }
                versions.append({
                    "content": content,
                    "tags": turn_tags,
                })

            sessions.append({
                "id": note_id,
                "tags": session_tags,
                "versions": versions,
                "turn_count": len(versions),
            })

    return sessions


# ── Image Notes ──────────────────────────────────────────────────────────

def build_image_notes(locomo: list) -> list[dict]:
    """Build per-image notes from all conversations.

    Only for turns that have img_url. Content = caption + query.
    ID = the URL itself.

    Returns list of dicts:
      {id, content, tags}
    """
    seen_urls: set[str] = set()
    notes = []

    for ci, item in enumerate(locomo):
        conv = item.get("conversation", {})
        speaker_a = conv.get("speaker_a", "A")
        speaker_b = conv.get("speaker_b", "B")

        for skey in _session_keys(conv):
            si = int(skey.split("_")[-1])
            for ti, turn in enumerate(conv.get(skey, []), 1):
                if "blip_caption" not in turn:
                    continue
                img_urls = turn.get("img_url", [])
                if not img_urls:
                    continue

                url = img_urls[0]
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                caption = turn.get("blip_caption", "")
                query = turn.get("query", "")
                speaker = turn.get("speaker", "?")
                dia_id = turn.get("dia_id", f"D{si}:{ti}")

                # Content: caption + search query
                content_parts = [caption]
                if query:
                    content_parts.append(f"Search: {query}")
                content = "\n".join(content_parts)

                tags = {
                    "type": "image",
                    "conv": str(ci),
                    "session": str(si),
                    "speaker": speaker,
                    "dia_id": dia_id,
                }

                notes.append({
                    "id": url,
                    "content": content,
                    "tags": tags,
                })

    return notes


# ── QA Dataset ───────────────────────────────────────────────────────────

def build_qa_dataset(locomo: list) -> list[dict]:
    """Build QA dataset for query/judge phases.

    Returns list of dicts:
      {conv, question, answer, evidence_refs, evidence_text, category}
    """
    qa_items = []

    for ci, item in enumerate(locomo):
        conv = item.get("conversation", {})
        for qa in item.get("qa", []):
            cat_id = qa.get("category")
            category = CATEGORY_NAMES.get(cat_id, f"category_{cat_id}")
            evidence_refs = qa.get("evidence", [])
            evidence_text = _parse_evidence(evidence_refs, conv)

            qa_items.append({
                "conv": ci,
                "question": qa.get("question", ""),
                "answer": qa.get("answer"),
                "evidence_refs": evidence_refs,
                "evidence_text": evidence_text,
                "category": category,
            })

    return qa_items


# ── Cognitive Dataset ────────────────────────────────────────────────────

def build_cognitive_dataset(plus: list) -> list[dict]:
    """Build cognitive dataset for Phase 2.

    Returns list of dicts with the raw fields needed for stitching + querying.
    """
    items = []
    for i, p in enumerate(plus):
        items.append({
            "idx": i,
            "cue_dialogue": p.get("cue_dialogue", ""),
            "trigger_query": p.get("trigger_query", ""),
            "time_gap": p.get("time_gap", ""),
            "relation_type": p.get("relation_type", ""),
            "scores": p.get("scores", {}),
            "final_similarity_score": p.get("final_similarity_score"),
        })
    return items


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Prepare LoCoMo-Plus dataset for keep benchmark")
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Path to Locomo-Plus/data/ directory")
    parser.add_argument("--out-dir", type=str, default="prepared",
                        help="Output directory (default: prepared/)")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load source data
    locomo_path = data_dir / "locomo10.json"
    plus_path = data_dir / "locomo_plus.json"

    if not locomo_path.exists():
        print(f"Error: {locomo_path} not found", file=sys.stderr)
        sys.exit(1)

    with open(locomo_path) as f:
        locomo = json.load(f)
    print(f"Loaded {len(locomo)} conversations from {locomo_path}")

    # ── Session notes ──
    session_notes = build_session_notes(locomo)
    session_path = out_dir / "session_notes.json"
    with open(session_path, "w") as f:
        json.dump(session_notes, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(session_notes)} session notes to {session_path}")

    # Session size stats
    sizes = [len(n["content"]) for n in session_notes]
    sizes.sort()
    n = len(sizes)
    print(f"  Session sizes — min:{sizes[0]} p50:{sizes[n//2]} p90:{sizes[9*n//10]} max:{sizes[-1]}")

    # ── Turn notes ──
    turn_notes = build_turn_notes(locomo)
    turn_path = out_dir / "turn_notes.json"
    with open(turn_path, "w") as f:
        json.dump(turn_notes, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(turn_notes)} turn notes to {turn_path}")

    # Turn size stats
    tsizes = [len(n["content"]) for n in turn_notes]
    tsizes.sort()
    tn = len(tsizes)
    print(f"  Turn sizes — min:{tsizes[0]} p50:{tsizes[tn//2]} p90:{tsizes[9*tn//10]} max:{tsizes[-1]}")

    # ── Versioned session notes (turns as versions) ──
    versioned = build_versioned_session_notes(locomo)
    versioned_path = out_dir / "versioned_session_notes.json"
    with open(versioned_path, "w") as f:
        json.dump(versioned, f, indent=2, ensure_ascii=False)
    total_versions = sum(s["turn_count"] for s in versioned)
    print(f"Wrote {len(versioned)} versioned sessions ({total_versions} total versions) to {versioned_path}")

    # ── Image notes ──
    image_notes = build_image_notes(locomo)
    image_path = out_dir / "image_notes.json"
    with open(image_path, "w") as f:
        json.dump(image_notes, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(image_notes)} image notes to {image_path}")

    # ── QA dataset ──
    qa_dataset = build_qa_dataset(locomo)
    qa_path = out_dir / "qa_dataset.json"
    with open(qa_path, "w") as f:
        json.dump(qa_dataset, f, indent=2, ensure_ascii=False)
    cats = Counter(q["category"] for q in qa_dataset)
    print(f"Wrote {len(qa_dataset)} QA items to {qa_path}")
    for cat, count in sorted(cats.items()):
        print(f"  {cat}: {count}")

    # ── Cognitive dataset ──
    if plus_path.exists():
        with open(plus_path) as f:
            plus = json.load(f)
        print(f"Loaded {len(plus)} cognitive samples from {plus_path}")

        cognitive = build_cognitive_dataset(plus)
        cog_path = out_dir / "cognitive_dataset.json"
        with open(cog_path, "w") as f:
            json.dump(cognitive, f, indent=2, ensure_ascii=False)
        rels = Counter(c["relation_type"] for c in cognitive)
        print(f"Wrote {len(cognitive)} cognitive items to {cog_path}")
        for rel, count in sorted(rels.items()):
            print(f"  {rel}: {count}")
    else:
        print(f"Skipping cognitive dataset ({plus_path} not found)")

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"Dataset prepared in {out_dir}/")
    print(f"  session_notes.json:           {len(session_notes)} notes (per-session)")
    print(f"  turn_notes.json:              {len(turn_notes)} notes (per-turn)")
    print(f"  versioned_session_notes.json: {len(versioned)} docs, {total_versions} versions (turns-as-versions)")
    print(f"  image_notes.json:             {len(image_notes)} notes")
    print(f"  qa_dataset.json:              {len(qa_dataset)} QA pairs")
    if plus_path.exists():
        print(f"  cognitive_dataset.json:        {len(cognitive)} cognitive samples")
    print(f"\n  Strategy note counts (+ {len(image_notes)} images):")
    print(f"    per-session:       {len(session_notes) + len(image_notes)} notes")
    print(f"    per-turn:          {len(turn_notes) + len(image_notes)} notes")
    print(f"    turns-as-versions: {len(versioned) + len(image_notes)} docs ({total_versions} put calls)")


if __name__ == "__main__":
    main()
