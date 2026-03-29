#!/usr/bin/env python3
# Copyright (c) 2026 Inguz Outcomes LLC.
"""LoCoMo-Plus benchmark — query keep and generate answers via LLM.

Uses keep's built-in .prompt/agent/query template with token-budgeted
context rendering.  The prompt, retrieval, and context assembly are all
handled by keep — this script just iterates over the QA dataset and
records predictions.

Usage:
    python query.py --store stores/run-001 --data-dir prepared/ --out results/run-001_predictions.json
"""

import argparse
import json
import sys
import time
from pathlib import Path

from keep.api import Keeper
from keep.cli import expand_prompt
from keep.config import load_config
from llm import create_llm


def load_store(store_path: Path) -> Keeper:
    """Load an existing keep store."""
    config = load_config(store_path)
    return Keeper(config=config)


def process_qa(kp: Keeper, llm, qa_dataset: list, top_k: int = 10,
               token_budget: int = 8000, deep: bool = False,
               out_path: Path = None, resume_from: int = 0) -> list[dict]:
    """Process all QA items. Supports resume via resume_from index."""
    predictions = []

    # Load existing predictions if resuming
    if resume_from > 0 and out_path and out_path.exists():
        predictions = json.loads(out_path.read_text())
        print(f"Resuming from index {resume_from} ({len(predictions)} existing predictions)")

    total = len(qa_dataset)
    for i, qa in enumerate(qa_dataset):
        if i < resume_from:
            continue

        question = qa["question"]
        conv = str(qa.get("conv", ""))
        tags = {"conv": conv} if conv else None

        # Use keep's built-in query prompt with token-budgeted context
        result = kp.render_prompt(
            "query", text=question,
            tags=tags, limit=top_k, deep=deep,
            token_budget=token_budget,
        )
        if result is None:
            prompt = f"Answer this question: {question}"
        else:
            prompt = expand_prompt(result, kp=kp)

        # Extract retrieval metadata from search results
        search_results = result.search_results if result else []
        retrieved_ids = [item.id for item in search_results] if search_results else []
        retrieved_scores = [item.score for item in search_results if item.score is not None] if search_results else []

        # Generate answer (with pacing to avoid rate limits)
        try:
            prediction = llm.generate(prompt, temperature=0.0)
        except Exception as e:
            prediction = f"ERROR: {e}"
        time.sleep(0.5)  # pace to stay under rate limits

        pred_record = {
            "idx": i,
            "conv": qa["conv"],
            "question": question,
            "answer": qa.get("answer"),
            "category": qa["category"],
            "evidence_text": qa.get("evidence_text", ""),
            "prediction": prediction,
            "retrieved_ids": retrieved_ids,
            "retrieved_scores": retrieved_scores,
            "top_score": retrieved_scores[0] if retrieved_scores else 0.0,
        }
        predictions.append(pred_record)

        if (i + 1) % 50 == 0 or i == total - 1:
            print(f"  query: {i+1}/{total} (cat={qa['category']})", flush=True)
            # Checkpoint: write partial results
            if out_path:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, "w") as f:
                    json.dump(predictions, f, indent=2, ensure_ascii=False)

    return predictions


def main():
    parser = argparse.ArgumentParser(description="Query keep for LoCoMo-Plus QA")
    parser.add_argument("--store", type=str, required=True,
                        help="Path to ingested keep store")
    parser.add_argument("--data-dir", type=str, default="prepared",
                        help="Path to prepared dataset")
    parser.add_argument("--out", type=str, required=True,
                        help="Output predictions JSON path")
    parser.add_argument("--top-k", type=int, default=10,
                        help="Number of results to retrieve (default: 10)")
    parser.add_argument("--tokens", type=int, default=8000,
                        help="Token budget for context (default: 8000)")
    parser.add_argument("--backend", type=str, default="openai",
                        choices=["openai", "gemini", "mlx"],
                        help="LLM backend (default: openai)")
    parser.add_argument("--model", type=str, default=None,
                        help="Model name (default: gpt-4o for openai)")
    parser.add_argument("--deep", action="store_true", default=False,
                        help="Enable deep tag-following in search (default: off)")
    parser.add_argument("--resume-from", type=int, default=0,
                        help="Resume from this QA index (for crash recovery)")
    args = parser.parse_args()

    store_path = Path(args.store).resolve()
    data_dir = Path(args.data_dir).resolve()
    out_path = Path(args.out).resolve()

    if not store_path.exists():
        print(f"ERROR: Store not found: {store_path}", file=sys.stderr)
        sys.exit(1)

    qa_dataset = json.loads((data_dir / "qa_dataset.json").read_text())
    print(f"Loaded {len(qa_dataset)} QA items")

    kp = load_store(store_path)
    llm = create_llm(args.backend, args.model)

    t0 = time.time()
    predictions = process_qa(kp, llm, qa_dataset, top_k=args.top_k,
                             token_budget=args.tokens, deep=args.deep,
                             out_path=out_path, resume_from=args.resume_from)
    elapsed = time.time() - t0

    # Final write
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(predictions, f, indent=2, ensure_ascii=False)

    print(f"\nQuery complete: {len(predictions)} predictions in {elapsed:.1f}s")
    print(f"Written to {out_path}")

    # Update STATE.json
    state_path = store_path / "STATE.json"
    if state_path.exists():
        state = json.loads(state_path.read_text())
        state["phases_completed"].append({
            "phase": "query",
            "completed": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "questions_answered": len(predictions),
            "top_k": args.top_k,
            "token_budget": args.tokens,
            "backend": args.backend,
            "model": args.model or "gpt-4o",
            "elapsed_seconds": round(elapsed, 1),
            "results_file": str(out_path),
        })
        with open(state_path, "w") as f:
            json.dump(state, f, indent=2)


if __name__ == "__main__":
    main()
