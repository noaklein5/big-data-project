"""Verify the Stage 10 query validator against gold queries and rejection cases."""

from __future__ import annotations

import sys

from src.ai.validator import ValidationError, validate_query
from src.elastic.gold_queries import load_gold_queries


def _expect_pass(index: str, body: dict, label: str) -> bool:
    try:
        validate_query(index, body, log_rejection=False)
    except ValidationError as exc:
        print(f"FAIL: {label} should pass — {exc}")
        return False
    print(f"OK   {label}")
    return True


def _expect_fail(index: str, body: dict, label: str) -> bool:
    try:
        validate_query(index, body, log_rejection=False)
    except ValidationError:
        print(f"OK   {label} (rejected as expected)")
        return True
    print(f"FAIL: {label} should have been rejected")
    return False


def main() -> int:
    passed = 0
    total = 0

    print("Gold queries (must pass validation):\n")
    for case in load_gold_queries():
        total += 1
        if _expect_pass(case["index"], case["body"], case["id"]):
            passed += 1

    print("\nRejection cases (must fail validation):\n")
    rejection_cases = [
        (
            "reject_forbidden_year_field",
            "movies",
            {"query": {"term": {"year": 2010}}, "size": 10},
        ),
        (
            "reject_oversized_result",
            "movies",
            {"query": {"match_all": {}}, "size": 500},
        ),
        (
            "reject_disallowed_top_level_key",
            "movies",
            {"query": {"match_all": {}}, "size": 10, "highlight": {}},
        ),
        (
            "reject_unknown_field",
            "movies",
            {"query": {"term": {"genre": "Comedy"}}, "size": 10},
        ),
        (
            "reject_field_on_wrong_index",
            "movies_by_release_year",
            {"query": {"term": {"genres": "Comedy"}}, "size": 10},
        ),
        (
            "reject_invalid_agg_type",
            "movies",
            {"size": 0, "aggs": {"bad": {"count": {"field": "genres"}}}},
        ),
        (
            "reject_script_clause",
            "movies",
            {
                "size": 10,
                "query": {"script_score": {"query": {"match_all": {}}, "script": "1"}},
            },
        ),
    ]

    for label, index, body in rejection_cases:
        total += 1
        if _expect_fail(index, body, label):
            passed += 1

    print(f"\n{passed}/{total} validator checks passed.")
    if passed == total:
        print("Query validator verification passed.")
        return 0

    print("Query validator verification failed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
