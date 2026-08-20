"""Evaluate AI query generation against Stage 8 gold questions."""

from __future__ import annotations

import argparse
import json
import sys
import time

from src.ai.generator import execute_query, generate_query
from src.ai.ollama_client import OllamaError
from src.ai.parser import ParseError
from src.ai.validator import ValidationError
from src.elastic.gold_queries import load_gold_queries


def _evaluate_case(case: dict) -> dict:
    question = case["question"]
    expected_index = case["index"]
    min_hits = case.get("min_hits", 0)
    min_agg = case.get("min_agg_buckets", 0)

    result = {
        "id": case["id"],
        "question": question,
        "expected_index": expected_index,
        "parsed": False,
        "validated": False,
        "index_ok": False,
        "executed": False,
        "results_ok": False,
        "error": None,
        "index": None,
        "hits": 0,
        "agg_buckets": 0,
    }

    try:
        generated = generate_query(question)
    except (OllamaError, ParseError, ValidationError, ValueError) as exc:
        result["error"] = str(exc)
        return result

    result["parsed"] = True
    result["validated"] = True
    result["index"] = generated.index
    result["index_ok"] = generated.index == expected_index

    try:
        exec_result = execute_query(generated)
    except Exception as exc:
        result["error"] = f"Elasticsearch error: {exc}"
        return result

    result["executed"] = True
    result["hits"] = exec_result.hits
    result["agg_buckets"] = exec_result.agg_buckets
    result["results_ok"] = exec_result.hits >= min_hits and exec_result.agg_buckets >= min_agg
    result["body"] = generated.body
    return result


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

    cases = load_gold_queries()
    if args.id:
        cases = [case for case in cases if case["id"] == args.id]
        if not cases:
            print(f"ERROR: Unknown query id: {args.id}")
            return 1

    print(f"Evaluating {len(cases)} gold question(s) with Ollama...\n")

    parsed = index_ok = executed = results_ok = 0
    start = time.perf_counter()

    last_outcome: dict | None = None

    for case in cases:
        outcome = _evaluate_case(case)
        last_outcome = outcome
        if outcome["parsed"]:
            parsed += 1
        if outcome["index_ok"]:
            index_ok += 1
        if outcome["executed"]:
            executed += 1
        if outcome["results_ok"]:
            results_ok += 1

        if outcome["error"]:
            status = "FAIL"
        elif outcome["results_ok"] and outcome["index_ok"]:
            status = "OK"
        elif outcome["executed"]:
            status = "WARN"
        else:
            status = "FAIL"

        print(f"[{status}] {outcome['id']}")
        print(f"       Q: {outcome['question']}")
        if outcome["error"]:
            print(f"       error: {outcome['error']}")
        else:
            index_note = "" if outcome["index_ok"] else f" (expected {outcome['expected_index']})"
            print(
                f"       index={outcome['index']}{index_note}  "
                f"hits={outcome['hits']:,}  agg_buckets={outcome['agg_buckets']}"
            )
            if args.show_dsl:
                print(
                    json.dumps(
                        {"index": outcome["index"], "body": outcome.get("body", {})},
                        indent=2,
                    )
                )
        print()

    elapsed = time.perf_counter() - start
    total = len(cases)
    print(
        f"Summary: parsed={parsed}/{total}  index={index_ok}/{total}  "
        f"executed={executed}/{total}  results_ok={results_ok}/{total}  "
        f"({elapsed:.1f}s)"
    )

    if args.id:
        o = last_outcome or {}
        return 0 if o.get("results_ok") and o.get("index_ok") and o.get("parsed") else 1

    min_results = max(1, total // 2)
    if parsed == total and results_ok >= min_results:
        print(f"AI evaluation passed ({results_ok}/{total} gold questions returned valid results).")
        return 0

    print("AI evaluation incomplete — review failures above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
