"""Verify Kibana Stage 12 dashboard setup."""

from __future__ import annotations

import sys

from src.kibana.verify_dashboard import verify_kibana_setup


def main() -> int:
    print("Verifying Kibana dashboard setup...\n")

    results = verify_kibana_setup()
    passed = 0
    for result in results:
        status = "OK" if result.passed else "FAIL"
        print(f"[{status}] {result.name}")
        print(f"       {result.detail}")
        if result.passed:
            passed += 1

    print(f"\n{passed}/{len(results)} checks passed.")
    if passed == len(results):
        print("Kibana dashboard verification passed.")
        print("Open http://localhost:5601 → Dashboards → MovieLens Analytics")
        return 0

    print("Kibana dashboard verification failed.")
    print("Run: docker exec movielens-app python scripts/setup_kibana.py")
    return 1


if __name__ == "__main__":
    sys.exit(main())
