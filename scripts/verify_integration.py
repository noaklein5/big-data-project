"""Run Stage 13 end-to-end integration verification."""

from __future__ import annotations

import argparse
import sys

from src.integration.verify import verify_integration


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify end-to-end MovieLens pipeline integration (Stage 13)"
    )
    parser.add_argument(
        "--mode",
        choices=("auto", "sample", "full"),
        default="auto",
        help="Expected pipeline mode for Kafka/ES thresholds (default: auto)",
    )
    parser.add_argument(
        "--include-ai",
        action="store_true",
        help="Run one Ollama smoke-test question (slower)",
    )
    args = parser.parse_args()

    print("Verifying end-to-end pipeline integration (Stage 13)...\n")

    try:
        expectations, results = verify_integration(
            mode=args.mode,
            include_ai=args.include_ai,
        )
    except Exception as exc:
        print(f"FAIL: {exc}")
        return 1

    passed = 0
    for result in results:
        status = "OK" if result.passed else "FAIL"
        print(f"[{status}] {result.name}")
        print(f"       {result.detail}")
        if result.passed:
            passed += 1

    print(f"\n{passed}/{len(results)} integration checks passed.")
    print(f"Mode: {expectations.mode}")

    if passed == len(results):
        print("Integration verification passed.")
        return 0

    print("Integration verification failed.")
    print("Ensure stack is up and pipeline ran: producer → Spark ETL → verify_es_indexes")
    return 1


if __name__ == "__main__":
    sys.exit(main())
