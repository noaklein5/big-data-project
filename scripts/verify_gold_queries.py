"""Run and verify Stage 8 gold Elasticsearch queries."""

from __future__ import annotations

import argparse
import json
import sys

from src.elastic.gold_queries import run_all_queries


def main() -> int:
    parser = argparse.ArgumentParser(description="Run gold Elasticsearch queries")
    parser.add_argument(
        "--id",
        help="Run a single query by id (e.g. movies_01)",
    )
    parser.add_argument(
        "--show-response",
        action="store_true",
        help="Print full Elasticsearch JSON response",
    )
    parser.add_argument(
        "--show-hits",
        type=int,
        default=3,
        help="When using --id, print first N hit documents (default: 3, 0 to skip)",
    )
    args = parser.parse_args()

    print("Running gold queries...\n")

    try:
        results = run_all_queries(query_id=args.id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1
    except Exception as exc:
        print(f"ERROR: {exc}")
        print("Ensure Docker stack is up and ETL data is loaded.")
        return 1

    passed = 0
    for result in results:
        status = "OK" if result["passed"] else "FAIL"
        print(f"[{status}] {result['id']}  ({result['index']})")
        print(f"       Q: {result['question']}")
        print(f"       hits={result['hits']:,}  agg_buckets={result['agg_buckets']}")

        if args.show_response:
            print(json.dumps(result["response"], indent=2, default=str))
        elif args.id and args.show_hits > 0:
            hits = result["response"]["hits"]["hits"][: args.show_hits]
            for index, hit in enumerate(hits, start=1):
                print(f"       [{index}] {json.dumps(hit['_source'], ensure_ascii=False)}")

        if args.id and result["response"].get("aggregations"):
            print(f"       aggs: {json.dumps(result['response']['aggregations'], indent=2, default=str)}")

        print()
        if result["passed"]:
            passed += 1

    if args.id:
        return 0 if results[0]["passed"] else 1

    print(f"{passed}/{len(results)} queries passed.")
    if passed == len(results):
        print("Gold query verification passed.")
        return 0

    print("Gold query verification failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
