"""Verify Stage 14 AI evaluation artifacts exist and meet minimum quality."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.ai.evaluation import DEFAULT_REPORT_PATH, DEFAULT_RESULTS_PATH, load_results_json

MIN_QUESTIONS = 20


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Stage 14 AI evaluation report")
    parser.add_argument(
        "--results-json",
        type=Path,
        default=DEFAULT_RESULTS_PATH,
        help="Path to ai_evaluation_results.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="Path to ai_evaluation.md",
    )
    parser.add_argument(
        "--min-semantic-rate",
        type=float,
        default=0.5,
        help="Minimum semantic correctness rate (default: 0.5)",
    )
    args = parser.parse_args()

    print("Verifying Stage 14 AI evaluation artifacts...\n")
    ok = True

    if not args.report.exists():
        print(f"[FAIL] report missing: {args.report}")
        print("       Run: docker exec movielens-app python scripts/run_ai_evaluation.py")
        return 1

    print(f"[OK] report exists: {args.report}")

    if not args.results_json.exists():
        print(f"[FAIL] results JSON missing: {args.results_json}")
        ok = False
    else:
        outcomes, metrics = load_results_json(args.results_json)
        print(f"[OK] results JSON: {args.results_json}")
        print(f"       questions={metrics.total}  semantic={metrics.semantic_correctness_rate * 100:.1f}%")

        if metrics.total < MIN_QUESTIONS:
            print(f"[FAIL] expected at least {MIN_QUESTIONS} questions, got {metrics.total}")
            ok = False
        else:
            print(f"[OK] question count >= {MIN_QUESTIONS}")

        if metrics.semantic_correctness_rate < args.min_semantic_rate:
            print(
                f"[FAIL] semantic correctness {metrics.semantic_correctness_rate * 100:.1f}% "
                f"< {args.min_semantic_rate * 100:.0f}%"
            )
            ok = False
        else:
            print(
                f"[OK] semantic correctness >= {args.min_semantic_rate * 100:.0f}% "
                f"({sum(1 for o in outcomes if o.semantic_ok)}/{metrics.total})"
            )

    if ok:
        print("\nStage 14 AI evaluation verification passed.")
        return 0

    print("\nStage 14 AI evaluation verification failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
