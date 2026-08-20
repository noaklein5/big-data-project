"""Run Stage 14 AI evaluation and write report artifacts."""

from __future__ import annotations

import argparse
import json
import sys
import time

from src.ai.evaluation import (
    DEFAULT_REPORT_PATH,
    DEFAULT_RESULTS_PATH,
    compute_metrics,
    evaluate_all_cases,
    format_report_markdown,
    save_results_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Stage 14 AI evaluation (20 NL questions)")
    parser.add_argument("--id", help="Evaluate a single query id (e.g. movies_02)")
    parser.add_argument(
        "--report",
        type=str,
        default=str(DEFAULT_REPORT_PATH),
        help=f"Markdown report path (default: {DEFAULT_REPORT_PATH})",
    )
    parser.add_argument(
        "--results-json",
        type=str,
        default=str(DEFAULT_RESULTS_PATH),
        help=f"JSON results path (default: {DEFAULT_RESULTS_PATH})",
    )
    parser.add_argument(
        "--show-dsl",
        action="store_true",
        help="Print generated DSL for each case",
    )
    parser.add_argument(
        "--min-semantic-rate",
        type=float,
        default=0.5,
        help="Minimum semantic correctness rate to exit 0 (default: 0.5)",
    )
    args = parser.parse_args()

    print("Running Stage 14 AI evaluation...\n")
    start = time.perf_counter()

    try:
        outcomes = evaluate_all_cases(query_id=args.id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    elapsed = time.perf_counter() - start
    metrics = compute_metrics(outcomes, elapsed)

    for outcome in outcomes:
        status = "OK" if outcome.semantic_ok else ("WARN" if outcome.executed else "FAIL")
        print(f"[{status}] {outcome.id} ({outcome.category})")
        print(f"       Q: {outcome.question}")
        if outcome.error:
            print(f"       error: {outcome.error}")
        else:
            index_note = "" if outcome.index_ok else f" (expected {outcome.expected_index})"
            print(
                f"       valid={outcome.valid_query}  index={outcome.index}{index_note}  "
                f"hits={outcome.hits:,}  agg={outcome.agg_buckets}  semantic={outcome.semantic_ok}"
            )
            if args.show_dsl and outcome.body:
                print(json.dumps({"index": outcome.index, "body": outcome.body}, indent=2))
        print()

    print(
        f"Summary ({metrics.total} questions):\n"
        f"  valid query rate:           {metrics.valid_query_rate * 100:.1f}%\n"
        f"  index selection accuracy:   {metrics.index_selection_accuracy * 100:.1f}%\n"
        f"  correct-result rate:        {metrics.correct_result_rate * 100:.1f}%\n"
        f"  semantic correctness:     {metrics.semantic_correctness_rate * 100:.1f}%\n"
        f"  elapsed:                    {metrics.elapsed_seconds:.1f}s"
    )

    if not args.id:
        report_path = DEFAULT_REPORT_PATH if args.report == str(DEFAULT_REPORT_PATH) else args.report
        results_path = (
            DEFAULT_RESULTS_PATH
            if args.results_json == str(DEFAULT_RESULTS_PATH)
            else args.results_json
        )
        from pathlib import Path

        save_results_json(outcomes, metrics, Path(results_path))
        report = format_report_markdown(outcomes, metrics)
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(report_path).write_text(report, encoding="utf-8")
        print(f"\nReport written to: {report_path}")
        print(f"Results JSON:      {results_path}")

    if args.id:
        o = outcomes[0]
        return 0 if o.semantic_ok else 1

    if metrics.semantic_correctness_rate >= args.min_semantic_rate:
        print(
            f"\nAI evaluation passed "
            f"(semantic correctness {metrics.semantic_correctness_rate * 100:.1f}% "
            f">= {args.min_semantic_rate * 100:.0f}%)."
        )
        return 0

    print("\nAI evaluation below threshold — review docs/ai_evaluation.md for details.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
