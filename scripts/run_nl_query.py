"""Ask a natural-language question and run the generated Elasticsearch query."""

from __future__ import annotations

import argparse
import json
import sys

from src.ai.generator import generate_and_execute
from src.ai.ollama_client import OllamaError
from src.ai.parser import ParseError
from src.ai.validator import ValidationError


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate and run an Elasticsearch query from natural language"
    )
    parser.add_argument(
        "question",
        nargs="?",
        help='Question in quotes, e.g. "Show Comedy movies released after 2000."',
    )
    parser.add_argument(
        "--show-response",
        action="store_true",
        help="Print the full Elasticsearch response",
    )
    parser.add_argument(
        "--show-dsl",
        action="store_true",
        help="Print the generated index and DSL body",
    )
    parser.add_argument(
        "--show-hits",
        type=int,
        default=5,
        help="Print first N hit documents (default: 5, 0 to skip)",
    )
    args = parser.parse_args()

    if not args.question:
        parser.error("question is required")

    print(f"Question: {args.question}\n")

    try:
        generated, result = generate_and_execute(args.question)
    except (OllamaError, ParseError, ValidationError, ValueError, RuntimeError) as exc:
        print(f"ERROR: {exc}")
        return 1

    print(f"Index: {generated.index}")
    print(f"Hits: {result.hits:,}  agg_buckets={result.agg_buckets}")

    if args.show_dsl:
        print("\nGenerated DSL:")
        print(json.dumps({"index": generated.index, "body": generated.body}, indent=2))

    if args.show_response:
        print("\nElasticsearch response:")
        print(json.dumps(result.response, indent=2, default=str))
    else:
        if args.show_hits > 0 and result.response["hits"]["hits"]:
            print("\nTop hits:")
            for index, hit in enumerate(result.response["hits"]["hits"][: args.show_hits], start=1):
                print(f"  [{index}] {json.dumps(hit['_source'], ensure_ascii=False)}")
        if result.response.get("aggregations"):
            print("\nAggregations:")
            print(json.dumps(result.response["aggregations"], indent=2, default=str))

    return 0


if __name__ == "__main__":
    sys.exit(main())
