#!/usr/bin/env python3
# Copyright (c) 2026 Inguz Outcomes LLC.
"""LoCoMo benchmark — binary LLM-as-judge scoring.

Uses the prompt from the Memobase/Mem0 evaluation harness:
  https://github.com/memodb-io/memobase/blob/main/docs/experiments/locomo-benchmark/metrics/llm_judge.py

This is the standard used by MemMachine, Memobase, Zep, Mem0, LangMem, OpenAI.
Category 5 (adversarial) is skipped per standard practice.
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

from openai import OpenAI

ACCURACY_PROMPT = """
Your task is to label an answer to a question as 'CORRECT' or 'WRONG'. You will be given the following data:
    (1) a question (posed by one user to another user), 
    (2) a 'gold' (ground truth) answer, 
    (3) a generated answer
which you will score as CORRECT/WRONG.

The point of the question is to ask about something one user should know about the other user based on their prior conversations.
The gold answer will usually be a concise and short answer that includes the referenced topic, for example:
Question: Do you remember what I got the last time I went to Hawaii?
Gold answer: A shell necklace
The generated answer might be much longer, but you should be generous with your grading - as long as it touches on the same topic as the gold answer, it should be counted as CORRECT. 

For time related questions, the gold answer will be a specific date, month, year, etc. The generated answer might be much longer or use relative time references (like "last Tuesday" or "next month"), but you should be generous with your grading - as long as it refers to the same date or time period as the gold answer, it should be counted as CORRECT. Even if the format differs (e.g., "May 7th" vs "7 May"), consider it CORRECT if it's the same date.

Now it's time for the real question:
Question: {question}
Gold answer: {gold_answer}
Generated answer: {generated_answer}

First, provide a short (one sentence) explanation of your reasoning, then finish with CORRECT or WRONG. 
Do NOT include both CORRECT and WRONG in your response, or it will break the evaluation script.

Just return the label CORRECT or WRONG in a json format with the key as "label".
"""


def evaluate_one(client, model, question, gold_answer, generated_answer):
    response = client.chat.completions.create(
        model=model,
        messages=[{
            "role": "user",
            "content": ACCURACY_PROMPT.format(
                question=question,
                gold_answer=gold_answer,
                generated_answer=generated_answer,
            ),
        }],
        response_format={"type": "json_object"},
        temperature=0.0,
    )
    label = json.loads(response.choices[0].message.content)["label"]
    return 1 if label == "CORRECT" else 0


def main():
    parser = argparse.ArgumentParser(description="Binary LLM judge (standard LoCoMo)")
    parser.add_argument("--predictions", type=str, required=True)
    parser.add_argument("--out", type=str, required=True)
    parser.add_argument("--model", type=str, default="gpt-4o-mini")
    parser.add_argument("--resume-from", type=int, default=0)
    args = parser.parse_args()

    pred_path = Path(args.predictions).resolve()
    out_path = Path(args.out).resolve()
    predictions = json.loads(pred_path.read_text())
    
    # Filter out adversarial (category 5) per standard practice
    non_adv = [p for p in predictions if p["category"] != "adversarial"]
    print(f"Loaded {len(predictions)} predictions, {len(non_adv)} non-adversarial to judge")

    client = OpenAI()

    judged = []
    if args.resume_from > 0 and out_path.exists():
        judged = json.loads(out_path.read_text())
        print(f"Resuming from {args.resume_from} ({len(judged)} existing)")

    t0 = time.time()
    total = len(non_adv)

    for i, pred in enumerate(non_adv):
        if i < args.resume_from:
            continue

        try:
            score = evaluate_one(
                client, args.model,
                question=pred.get("question", ""),
                gold_answer=str(pred.get("answer", "")),
                generated_answer=pred.get("prediction", ""),
            )
        except Exception as e:
            print(f"  Error at {i}: {e}")
            score = 0

        record = {**pred, "score": score}
        judged.append(record)

        if (i + 1) % 50 == 0 or i == total - 1:
            cats = defaultdict(list)
            for j in judged:
                cats[j["category"]].append(j["score"])
            summary = ", ".join(f"{c}:{sum(s)/len(s)*100:.0f}%" for c, s in sorted(cats.items()))
            overall = sum(j["score"] for j in judged) / len(judged) * 100
            print(f"  {i+1}/{total} — {summary} | overall: {overall:.1f}%", flush=True)

            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w") as f:
                json.dump(judged, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - t0
    with open(out_path, "w") as f:
        json.dump(judged, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"Binary judging complete: {len(judged)} items in {elapsed:.1f}s")
    cats = defaultdict(list)
    for j in judged:
        cats[j["category"]].append(j["score"])

    total_q = total_c = 0
    for cat in ["single-hop", "multi-hop", "temporal", "open-domain"]:
        scores = cats.get(cat, [])
        if scores:
            avg = sum(scores)/len(scores)*100
            print(f"  {cat:15s}: {avg:5.1f}% ({len(scores)} questions)")
            total_q += len(scores)
            total_c += sum(scores)

    # Weighted overall (matches MemMachine methodology)
    print(f"  {'Overall':15s}: {total_c/total_q*100:5.1f}% (n={total_q})")

    summary_path = out_path.with_suffix(".summary.json")
    summary = {
        "total": len(judged), "elapsed_seconds": round(elapsed, 1),
        "model": args.model,
        "judge_prompt": "memobase/mem0 standard ACCURACY_PROMPT",
        "scoring": "binary (CORRECT=1, WRONG=0)",
        "adversarial": "excluded (standard practice)",
        "categories": {
            cat: {"count": len(s), "llm_score": round(sum(s)/len(s), 4)}
            for cat, s in sorted(cats.items())
        },
        "overall_weighted": round(total_c/total_q, 4),
    }
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary → {summary_path}")


if __name__ == "__main__":
    main()
