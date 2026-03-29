#!/usr/bin/env python3
"""Run flow-based retrieval traces on LoCoMo QA items.

Writes a full per-step log with exact continue payloads and outputs so the
interaction can be inspected post-run.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from keep.api import Keeper
from keep.config import load_config


def _load_keeper(store_path: Path) -> Keeper:
    config = load_config(store_path)
    return Keeper(config=config)


def _dominant_conv(evidence: list[dict[str, Any]]) -> str | None:
    counts: dict[str, int] = {}
    for row in evidence:
        metadata = row.get("metadata") if isinstance(row, dict) else None
        tags = metadata.get("tags") if isinstance(metadata, dict) else None
        if not isinstance(tags, dict):
            continue
        conv_val = tags.get("conv")
        if conv_val is None:
            continue
        conv = str(conv_val)
        if not conv:
            continue
        counts[conv] = counts.get(conv, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda kv: kv[1])[0]


def _summarize_output(output: dict[str, Any]) -> dict[str, Any]:
    frame = output.get("frame") if isinstance(output, dict) else {}
    views = frame.get("views") if isinstance(frame, dict) else {}
    evidence = views.get("evidence") if isinstance(views, dict) else []
    return {
        "status": output.get("status"),
        "state_version": output.get("state_version"),
        "flow_id": output.get("flow_id"),
        "error_codes": [e.get("code") for e in output.get("errors", []) if isinstance(e, dict)],
        "work_count": len(output.get("requests", {}).get("work", [])),
        "evidence_count": len(evidence) if isinstance(evidence, list) else 0,
        "top_ids": [row.get("id") for row in (evidence or [])[:5] if isinstance(row, dict)],
    }


def _run_continue(
    kp: Keeper, payload: dict[str, Any], run_log: dict[str, Any], *, label: str,
) -> dict[str, Any]:
    t0 = time.time()
    output = kp.continue_flow(payload)
    elapsed_ms = round((time.time() - t0) * 1000, 2)
    run_log["steps"].append(
        {
            "label": label,
            "type": "continue",
            "request": payload,
            "response": output,
            "response_summary": _summarize_output(output),
            "elapsed_ms": elapsed_ms,
        }
    )
    return output


def _run_until_settled(
    kp: Keeper,
    first_output: dict[str, Any],
    run_log: dict[str, Any],
    *,
    request_prefix: str,
    max_ticks: int = 6,
) -> dict[str, Any]:
    output = first_output
    tick_num = 0
    while tick_num < max_ticks:
        tick_num += 1
        status = str(output.get("status") or "")
        if status != "waiting_work":
            return output
        requested = output.get("requests", {}).get("work", [])
        if not isinstance(requested, list) or not requested:
            return output
        work_results = []
        for wr in requested:
            if not isinstance(wr, dict):
                continue
            work_id = str(wr.get("work_id") or "")
            if not work_id:
                continue
            flow_id = str(output.get("flow_id") or "")
            t0 = time.time()
            work_result = kp.continue_run_work(flow_id, work_id)
            elapsed_ms = round((time.time() - t0) * 1000, 2)
            run_log["steps"].append(
                {
                    "label": f"work_{tick_num}_{work_id}",
                    "type": "run_work",
                    "request": {"flow_id": flow_id, "work_id": work_id},
                    "response": work_result,
                    "elapsed_ms": elapsed_ms,
                }
            )
            work_results.append(work_result)

        continue_payload = {
            "schema_version": "continue.v1",
            "request_id": f"{request_prefix}-work-{tick_num}",
            "flow_id": output.get("flow_id"),
            "state_version": output.get("state_version"),
            "feedback": {"work_results": work_results},
        }
        output = _run_continue(kp, continue_payload, run_log, label=f"tick_work_{tick_num}")
    return output


def run_trace(
    *,
    store_path: Path,
    qa_path: Path,
    out_path: Path,
    count: int,
    offset: int,
    limit: int,
    token_budget: int,
    deep: bool,
    metadata: str,
) -> dict[str, Any]:
    qa_dataset = json.loads(qa_path.read_text())
    selected = qa_dataset[offset: offset + count]

    trace: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "store_path": str(store_path),
        "qa_path": str(qa_path),
        "count": len(selected),
        "offset": offset,
        "settings": {
            "limit": limit,
            "token_budget": token_budget,
            "deep": deep,
            "metadata": metadata,
        },
        "runs": [],
    }

    kp = _load_keeper(store_path)
    try:
        for local_idx, qa in enumerate(selected):
            qa_idx = offset + local_idx
            question = str(qa.get("question") or "")
            expected_conv = str(qa.get("conv") or "")

            run_log: dict[str, Any] = {
                "qa_index": qa_idx,
                "conv": expected_conv,
                "category": qa.get("category"),
                "question": question,
                "gold_answer": qa.get("answer"),
                "steps": [],
            }

            first_payload = {
                "schema_version": "continue.v1",
                "request_id": f"locomo-{qa_idx}-tick-1",
                "goal": "query",
                "params": {"text": question},
                "frame_request": {
                    "seed": {"mode": "query", "value": question},
                    "pipeline": [
                        {"op": "slice", "args": {"limit": limit}},
                    ],
                    "budget": {"tokens": token_budget, "max_nodes": limit},
                    "options": {"deep": False, "metadata": metadata},
                },
                "feedback": {"work_results": []},
            }
            first_output = _run_continue(kp, first_payload, run_log, label="tick_1_broad")
            first_output = _run_until_settled(
                kp, first_output, run_log, request_prefix=f"locomo-{qa_idx}-broad",
            )

            evidence = first_output.get("frame", {}).get("views", {}).get("evidence", [])
            dominant_conv = _dominant_conv(evidence if isinstance(evidence, list) else [])
            selected_conv = dominant_conv or expected_conv
            run_log["agent_eval"] = {
                "dominant_conv_from_tick_1": dominant_conv,
                "selected_conv_for_tick_2": selected_conv,
            }

            if selected_conv:
                second_payload = {
                    "schema_version": "continue.v1",
                    "request_id": f"locomo-{qa_idx}-tick-2",
                    "flow_id": first_output.get("flow_id"),
                    "state_version": first_output.get("state_version"),
                    "frame_request": {
                        "seed": {"mode": "query", "value": question},
                        "pipeline": [
                            {"op": "where", "args": {"facts": [f"conv={selected_conv}"]}},
                            {"op": "slice", "args": {"limit": limit}},
                        ],
                        "budget": {"tokens": token_budget, "max_nodes": limit},
                        "options": {"deep": deep, "metadata": metadata},
                    },
                    "feedback": {"work_results": []},
                }
                second_output = _run_continue(kp, second_payload, run_log, label="tick_2_refine")
                second_output = _run_until_settled(
                    kp, second_output, run_log, request_prefix=f"locomo-{qa_idx}-refine",
                )
                run_log["final"] = _summarize_output(second_output)
            else:
                run_log["final"] = _summarize_output(first_output)

            trace["runs"].append(run_log)
    finally:
        kp.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(trace, indent=2, ensure_ascii=False))
    return trace


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate flow traces on LoCoMo queries")
    parser.add_argument("--store", required=True, help="Path to keep store")
    parser.add_argument("--qa", required=True, help="Path to qa_dataset.json")
    parser.add_argument("--out", required=True, help="Output trace JSON path")
    parser.add_argument("--count", type=int, default=10, help="Number of QA entries")
    parser.add_argument("--offset", type=int, default=0, help="Starting QA index")
    parser.add_argument("--limit", type=int, default=10, help="Evidence limit per tick")
    parser.add_argument("--tokens", type=int, default=2400, help="Frame token budget")
    parser.add_argument("--deep", action="store_true", default=False, help="Use deep search on refined tick")
    parser.add_argument(
        "--metadata",
        choices=["none", "basic", "rich"],
        default="rich",
        help="Evidence metadata level",
    )
    args = parser.parse_args()

    trace = run_trace(
        store_path=Path(args.store).resolve(),
        qa_path=Path(args.qa).resolve(),
        out_path=Path(args.out).resolve(),
        count=args.count,
        offset=args.offset,
        limit=args.limit,
        token_budget=args.tokens,
        deep=args.deep,
        metadata=args.metadata,
    )
    print(
        f"Wrote trace with {len(trace.get('runs', []))} runs to {Path(args.out).resolve()}",
        flush=True,
    )


if __name__ == "__main__":
    main()
