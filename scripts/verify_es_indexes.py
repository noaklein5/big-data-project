"""Run Stage 7 Elasticsearch index and query verification."""

from __future__ import annotations

import sys

from src.elastic.verify_indexes import run_all_checks


def main() -> int:
    print("Verifying Elasticsearch indexes, mappings, and queries...\n")

    try:
        results = run_all_checks()
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

    print(f"\n{passed}/{len(results)} checks passed.")

    if passed == len(results):
        print("Elasticsearch index verification passed.")
        return 0

    print("Elasticsearch index verification failed.")
    print("Ensure setup + ETL pipeline ran: setup_infrastructure → producer → spark ETL")
    return 1


if __name__ == "__main__":
    sys.exit(main())
