"""Validate the locked schema module."""

from __future__ import annotations

import sys

from src.config import (
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
)
from src.elastic.schema import (
    ALL_INDEX_NAMES,
    DOCUMENT_ID_FIELDS,
    FORBIDDEN_FIELDS,
    INDEX_FIELDS,
    INDEX_MAPPINGS,
    SPARK_WRITE_CONFIG,
    format_document_id,
    get_allowed_fields,
    get_llm_schema_prompt,
)


def main() -> int:
    checks_passed = True

    for index_name in ALL_INDEX_NAMES:
        mapping_props = set(
            INDEX_MAPPINGS[index_name]["mappings"]["properties"].keys()
        )
        schema_fields = set(INDEX_FIELDS[index_name].keys())
        if mapping_props != schema_fields:
            print(f"FAIL: field mismatch in {index_name}")
            print(f"  mappings only: {mapping_props - schema_fields}")
            print(f"  schema only:   {schema_fields - mapping_props}")
            checks_passed = False
        if index_name not in SPARK_WRITE_CONFIG:
            print(f"FAIL: missing Spark write config for {index_name}")
            checks_passed = False

    sample_ids = {
        INDEX_MOVIES: format_document_id(INDEX_MOVIES, {"movie_id": 42}),
        INDEX_MOVIES_BY_RELEASE_YEAR: format_document_id(
            INDEX_MOVIES_BY_RELEASE_YEAR, {"release_year": 2010}
        ),
        INDEX_MOVIE_RATINGS_BY_RATING_YEAR: format_document_id(
            INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
            {"movie_rating_year_id": "1_2010"},
        ),
    }
    expected = {"42", "2010", "1_2010"}
    if set(sample_ids.values()) != expected:
        print(f"FAIL: unexpected document IDs: {sample_ids}")
        checks_passed = False

    if "year" in get_allowed_fields(INDEX_MOVIES):
        print("FAIL: forbidden field `year` is allowed")
        checks_passed = False

    prompt = get_llm_schema_prompt()
    if "release_year" not in prompt or "rating_year" not in prompt:
        print("FAIL: LLM schema prompt missing year fields")
        checks_passed = False

    if checks_passed:
        print("Schema validation passed.")
        print(f"Indexes: {', '.join(ALL_INDEX_NAMES)}")
        print(f"Forbidden fields: {', '.join(sorted(FORBIDDEN_FIELDS))}")
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
