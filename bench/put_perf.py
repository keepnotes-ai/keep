#!/usr/bin/env python3
"""Benchmark the put → process pipeline.

Measures end-to-end ingestion: put call latency, background processing
time (summarize, embed, tag, analyze, edges), and throughput.

Usage:
    python bench/put_perf.py                    # real providers, temp store
    python bench/put_perf.py --mock             # mock providers (fast, repeatable)
    python bench/put_perf.py --store ~/.keep    # existing store (careful!)

Items are put into a fresh temp store by default. The benchmark waits
for all background processing to complete before reporting.
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path


ITEMS = {
    "short_text": {
        "content": "Decision: use JWT tokens for API auth instead of session cookies.",
        "tags": {"project": "auth", "act": "assertion"},
        "desc": "Short text (below summarize threshold)",
    },
    "long_text": {
        "content": (
            "# Meeting Notes: Architecture Review\n\n"
            "## Attendees\nAlice, Bob, Carol\n\n"
            "## Discussion\n\n"
            "We reviewed the current caching strategy and identified several issues:\n\n"
            "1. The cache invalidation is too aggressive — every write flushes all entries.\n"
            "2. The cache key doesn't include the embedding model, so model changes serve stale results.\n"
            "3. There's no TTL, so entries live forever until explicitly invalidated.\n\n"
            "## Decisions\n\n"
            "- Implement per-component cache with targeted invalidation\n"
            "- Add generation counter for similar-item cache with 60s TTL\n"
            "- Cache IDs+scores only, hydrate Items fresh on read\n"
            "- Start with meta cache (biggest win: 965ms → <1ms)\n\n"
            "## Action Items\n\n"
            "- [ ] Alice: write design doc for component cache\n"
            "- [ ] Bob: benchmark get_context components\n"
            "- [ ] Carol: review meta-doc dependency extraction\n"
        ),
        "tags": {"project": "cache", "type": "meeting"},
        "desc": "Long text (triggers summarize + embed + tag)",
    },
    "markdown_file": {
        "desc": "Markdown file (read + summarize + embed)",
    },
    "reput_same": {
        "content": "Decision: use JWT tokens for API auth instead of session cookies.",
        "tags": {"project": "auth", "act": "assertion"},
        "desc": "Re-put same content (dedup — should be near-zero work)",
    },
    "text_with_analysis": {
        "content": (
            "# Caching Strategy Analysis\n\n"
            "## Current State\n\n"
            "The system has no caching layer. Every get_context() call runs:\n"
            "- Vector similarity search (39ms)\n"
            "- Meta-doc resolution with LLM calls (965ms)\n"
            "- Edge tag traversal (0.1ms)\n"
            "- Parts listing (30ms)\n"
            "- Version navigation (0.3ms)\n\n"
            "## Proposed Solution\n\n"
            "Action-level cache in the flow engine. Three component managers:\n\n"
            "### SimilarCache\n"
            "Generation + TTL (60s). On any write, bump generation.\n"
            "Cached entries with old generation served if TTL hasn't expired.\n\n"
            "### MetaCache\n"
            "Tag dependency graph extracted from meta-doc YAML.\n"
            "On write, check if written item's tags intersect any meta dependency.\n\n"
            "### PartsCache\n"
            "Per-item content hash. Invalidate on re-analyze.\n\n"
            "## Expected Impact\n\n"
            "Warm cache: get_context drops from 1104ms to ~5ms (200x speedup).\n"
        ),
        "tags": {"project": "cache", "type": "analysis"},
        "desc": "Long analysis doc (triggers analyze in addition to summarize)",
    },
}


def _create_temp_markdown(tmp_dir: Path) -> Path:
    """Create a temp markdown file for file-mode put."""
    p = tmp_dir / "bench-test-doc.md"
    p.write_text(
        "# Benchmark Test Document\n\n"
        "This file tests the file-mode ingestion path.\n\n"
        "## Section 1\n\nSome content about caching strategies.\n\n"
        "## Section 2\n\nMore content about performance optimization.\n"
    )
    return p


def _wait_for_completion(kp, timeout: float = 120) -> float:
    """Wait for all pending+flow work to complete. Returns elapsed seconds."""
    t0 = time.monotonic()
    deadline = t0 + timeout
    while time.monotonic() < deadline:
        pending = kp._pending_queue.count()
        flow_pending = kp.pending_work_count() if hasattr(kp, "pending_work_count") else 0
        processing = kp._pending_queue.stats().get("processing", 0)
        if pending == 0 and flow_pending == 0 and processing == 0:
            return time.monotonic() - t0
        # Process one batch
        kp.process_pending(limit=5)
        if hasattr(kp, "process_pending_work"):
            kp.process_pending_work(limit=5, worker_id="bench", lease_seconds=60)
    return time.monotonic() - t0


def main():
    parser = argparse.ArgumentParser(description="Benchmark put → process pipeline")
    parser.add_argument("--store", type=str, default=None, help="Store path (default: temp)")
    parser.add_argument("--mock", action="store_true", help="Use mock providers (fast, repeatable)")
    parser.add_argument("--skip-analyze", action="store_true", help="Skip analysis-triggering items")
    args = parser.parse_args()

    # Setup store
    if args.store:
        store_path = Path(args.store)
        tmp_dir_obj = None
    else:
        tmp_dir_obj = tempfile.TemporaryDirectory(prefix="keep-bench-")
        store_path = Path(tmp_dir_obj.name)

    print(f"Store: {store_path}", file=sys.stderr)
    print(f"Mode: {'mock' if args.mock else 'real'} providers", file=sys.stderr)

    if args.mock:
        # Mock providers — reuse the test fixture pattern
        import os
        os.environ["KEEP_STORE_PATH"] = str(store_path)
        os.environ["KEEP_CONFIG"] = str(store_path)

        # Add tests/ to path so conftest is importable
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests"))
        from conftest import MockEmbeddingProvider, MockSummarizationProvider, MockDocumentProvider
        from unittest.mock import MagicMock, patch

        mock_embed = MockEmbeddingProvider()
        mock_summ = MockSummarizationProvider()
        mock_doc = MockDocumentProvider()
        mock_reg = MagicMock()
        mock_reg.create_document.return_value = mock_doc
        mock_reg.create_embedding.return_value = mock_embed
        mock_reg.create_summarization.return_value = mock_summ

        _patches = [
            patch("keep.api.get_registry", return_value=mock_reg),
            patch("keep._provider_lifecycle.get_registry", return_value=mock_reg),
            patch("keep.api.CachingEmbeddingProvider", side_effect=lambda p, **kw: p),
            patch("keep._provider_lifecycle.CachingEmbeddingProvider", side_effect=lambda p, **kw: p),
        ]
        for p in _patches:
            p.start()

        from keep.api import Keeper
        kp = Keeper(store_path=store_path)
    else:
        from keep.api import Keeper
        kp = Keeper(store_path=store_path)

    # Ensure system docs are loaded and background work drained
    if kp._needs_sysdoc_migration:
        kp._migrate_system_documents()
        kp._needs_sysdoc_migration = False
    _wait_for_completion(kp, timeout=30)

    # Create temp files
    md_file = _create_temp_markdown(store_path)

    # --- Benchmark ---
    print(f"\n{'Item':<25} {'Put':>8} {'Process':>10} {'Total':>10}  Description", file=sys.stderr)
    print("-" * 85, file=sys.stderr)

    results = {}
    for name, spec in ITEMS.items():
        if args.skip_analyze and name == "text_with_analysis":
            continue

        # Prepare args
        put_kwargs = {}
        if name == "markdown_file":
            put_kwargs["uri"] = f"file://{md_file}"
        else:
            put_kwargs["content"] = spec["content"]
            if name == "reput_same":
                put_kwargs["id"] = "short_text_reput"
            else:
                put_kwargs["id"] = f"bench-{name}"
        if "tags" in spec:
            put_kwargs["tags"] = spec["tags"]

        # Put (synchronous)
        t_put_start = time.perf_counter()
        item = kp.put(**put_kwargs)
        t_put = time.perf_counter() - t_put_start

        # Process all pending work
        t_proc_start = time.perf_counter()
        proc_elapsed = _wait_for_completion(kp)
        t_proc = time.perf_counter() - t_proc_start

        total = t_put + t_proc
        results[name] = {
            "put_ms": t_put * 1000,
            "process_ms": t_proc * 1000,
            "total_ms": total * 1000,
            "desc": spec["desc"],
        }

        print(
            f"{name:<25} {t_put*1000:7.1f}ms {t_proc*1000:9.1f}ms {total*1000:9.1f}ms  {spec['desc']}",
            file=sys.stderr,
        )

    # Summary
    print("\n" + "=" * 85, file=sys.stderr)
    total_put = sum(r["put_ms"] for r in results.values())
    total_proc = sum(r["process_ms"] for r in results.values())
    total_all = sum(r["total_ms"] for r in results.values())
    n = len(results)
    print(f"{'TOTAL':<25} {total_put:7.1f}ms {total_proc:9.1f}ms {total_all:9.1f}ms  ({n} items)", file=sys.stderr)
    print(f"{'PER ITEM (avg)':<25} {total_put/n:7.1f}ms {total_proc/n:9.1f}ms {total_all/n:9.1f}ms", file=sys.stderr)

    # Per-action perf stats
    try:
        from keep.perf_stats import perf
        lines = perf.format_summary()
        if lines:
            print(f"\nPerf stats:\n{lines}", file=sys.stderr)
    except Exception:
        pass

    # Output JSON for programmatic comparison
    import json
    print(json.dumps(results, indent=2))

    kp.close()
    if tmp_dir_obj:
        tmp_dir_obj.cleanup()


if __name__ == "__main__":
    main()
