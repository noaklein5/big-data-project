"""Evaluate AI query generation against gold questions (Stage 9/14)."""

from __future__ import annotations

import argparse
import json
import sys
import time

from src.ai.evaluation import compute_metrics, evaluate_all_cases


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate NL → ES query generation on gold questions"
    )
    parser.add_argument("--id", help="Evaluate a single gold query id (e.g. movies_01)")
    parser.add_argument(
        "--show-dsl",
        action="store_true",
        help="Print generated DSL for each case",
    )
    args = parser.parse_args()

    print(f"Evaluating gold question(s) with Ollama...\n")
    start = time.perf_counter()

    try:
        outcomes = evaluate_all_cases(query_id=args.id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    metrics = compute_metrics(outcomes, time.perf_counter() - start)

    for outcome in outcomes:
        if outcome.error:
            status = "FAIL"
        elif outcome.semantic_ok:
            status = "OK"
        elif outcome.executed:
            status = "WARN"
        else:
            status = "FAIL"

        print(f"[{status}] {outcome.id}")
        print(f"       Q: {outcome.question}")
        if outcome.error:
            print(f"       error: {outcome.error}")
        else:
            index_note = "" if outcome.index_ok else f" (expected {outcome.expected_index})"
            print(
                f"       index={outcome.index}{index_note}  "
                f"hits={outcome.hits:,}  agg_buckets={outcome.agg_buckets}"
            )
            if args.show_dsl and outcome.body:
                print(
                    json.dumps(
                        {"index": outcome.index, "body": outcome.body},
                        indent=2,
                    )
                )
        print()

    print(
        f"Summary: valid={sum(1 for o in outcomes if o.valid_query)}/{metrics.total}  "
        f"index={sum(1 for o in outcomes if o.index_ok)}/{metrics.total}  "
        f"results={sum(1 for o in outcomes if o.results_ok)}/{metrics.total}  "
        f"semantic={sum(1 for o in outcomes if o.semantic_ok)}/{metrics.total}  "
        f"({metrics.elapsed_seconds:.1f}s)"
    )

    if args.id:
        o = outcomes[0]
        return 0 if o.semantic_ok else 1

    min_results = max(1, metrics.total // 2)
    semantic_ok_count = sum(1 for o in outcomes if o.semantic_ok)
    if metrics.valid_query_rate == 1.0 and semantic_ok_count >= min_results:
        print(f"AI evaluation passed ({semantic_ok_count}/{metrics.total} semantically correct).")
        return 0

    print("AI evaluation incomplete — run scripts/run_ai_evaluation.py for full Stage 14 report.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
