"""Validate LLM-generated Elasticsearch queries before execution."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from src.elastic.schema import (
    ALL_INDEX_NAMES,
    FORBIDDEN_FIELDS,
    get_allowed_fields,
)

logger = logging.getLogger(__name__)

MAX_RESULT_SIZE = 100

ALLOWED_TOP_LEVEL_KEYS = frozenset({"query", "aggs", "sort", "size", "_source"})

FORBIDDEN_ANYWHERE_KEYS = frozenset(
    {
        "script",
        "script_fields",
        "stored_script",
        "delete_by_query",
        "update_by_query",
        "bulk",
        "index",
        "create",
        "update",
        "delete",
    }
)

QUERY_CLAUSE_TYPES = frozenset(
    {
        "term",
        "terms",
        "range",
        "match",
        "match_phrase",
        "match_phrase_prefix",
        "multi_match",
        "wildcard",
        "prefix",
        "exists",
        "regexp",
        "fuzzy",
        "ids",
    }
)

BOOL_SECTIONS = frozenset({"must", "filter", "should", "must_not"})

ALLOWED_AGG_TYPES = frozenset(
    {
        "terms",
        "avg",
        "sum",
        "min",
        "max",
        "value_count",
        "stats",
        "histogram",
        "date_histogram",
        "range",
        "date_range",
        "filter",
        "filters",
        "cardinality",
        "top_hits",
        "nested",
        "reverse_nested",
        "global",
        "missing",
        "bucket_sort",
    }
)

SUBFIELD_SUFFIXES = (".keyword", ".raw")


class ValidationError(ValueError):
    """Raised when a query fails safety or schema validation."""

    def __init__(self, message: str, *, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors = errors or [message]


@dataclass(frozen=True)
class ValidatedQuery:
    index: str
    body: dict


def _normalize_field(field_name: str) -> str:
    for suffix in SUBFIELD_SUFFIXES:
        if field_name.endswith(suffix):
            return field_name[: -len(suffix)]
    return field_name


def _collect_field_references(value: Any) -> set[str]:
    fields: set[str] = set()

    def walk(node: Any, *, in_aggs: bool = False) -> None:
        if isinstance(node, dict):
            for key, nested in node.items():
                if key in FORBIDDEN_ANYWHERE_KEYS:
                    continue
                if key in {"aggs", "aggregations"}:
                    walk(nested, in_aggs=True)
                elif key == "field" and isinstance(nested, str):
                    if nested not in FORBIDDEN_FIELDS:
                        fields.add(nested)
                elif key == "fields" and isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, str) and item not in FORBIDDEN_FIELDS:
                            fields.add(item)
                elif key == "sort" and isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, str):
                            fields.add(item)
                        elif isinstance(item, dict):
                            for field_name in item:
                                if field_name not in {"_score", "_doc"}:
                                    fields.add(field_name)
                elif not in_aggs and key in QUERY_CLAUSE_TYPES and isinstance(nested, dict):
                    for field_name in nested:
                        if field_name not in FORBIDDEN_FIELDS:
                            fields.add(field_name)
                elif key in BOOL_SECTIONS or key in {"query", "bool"}:
                    walk(nested, in_aggs=in_aggs)
                elif in_aggs and key in ALLOWED_AGG_TYPES:
                    walk(nested, in_aggs=True)
                else:
                    walk(nested, in_aggs=in_aggs)
        elif isinstance(node, list):
            for item in node:
                walk(item, in_aggs=in_aggs)

    walk(value)
    return fields


def _find_forbidden_keys(value: Any, path: str = "") -> list[str]:
    hits: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            current = f"{path}.{key}" if path else key
            if key in FORBIDDEN_FIELDS:
                hits.append(current)
            if key in FORBIDDEN_ANYWHERE_KEYS:
                hits.append(f"{current} (forbidden operation/key)")
            hits.extend(_find_forbidden_keys(nested, current))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            hits.extend(_find_forbidden_keys(item, f"{path}[{index}]"))
    return hits


def _validate_agg_types(aggs: dict, path: str = "aggs") -> list[str]:
    errors: list[str] = []
    for agg_name, definition in aggs.items():
        if not isinstance(definition, dict):
            errors.append(f"{path}.{agg_name} must be an object")
            continue
        agg_types = [
            key
            for key in definition
            if key not in {"aggs", "aggregations", "meta"}
        ]
        if not agg_types:
            errors.append(f"{path}.{agg_name} missing aggregation type")
            continue
        for agg_type in agg_types:
            if agg_type not in ALLOWED_AGG_TYPES:
                errors.append(
                    f"{path}.{agg_name} uses disallowed aggregation type `{agg_type}`"
                )
        nested = definition.get("aggs") or definition.get("aggregations")
        if isinstance(nested, dict):
            errors.extend(_validate_agg_types(nested, f"{path}.{agg_name}"))
    return errors


def _validate_top_level(body: dict) -> list[str]:
    errors: list[str] = []
    unknown = set(body) - ALLOWED_TOP_LEVEL_KEYS
    if unknown:
        errors.append(
            f"Disallowed top-level keys: {', '.join(sorted(unknown))}. "
            f"Allowed: {', '.join(sorted(ALLOWED_TOP_LEVEL_KEYS))}"
        )
    if "size" in body:
        size = body["size"]
        if not isinstance(size, int) or isinstance(size, bool):
            errors.append('"size" must be an integer')
        elif size < 0:
            errors.append('"size" must be >= 0')
        elif size > MAX_RESULT_SIZE:
            errors.append(f'"size" must be <= {MAX_RESULT_SIZE}')
    return errors


def _validate_fields_for_index(index: str, body: dict) -> list[str]:
    allowed = get_allowed_fields(index)
    referenced = _collect_field_references(body)
    errors: list[str] = []
    for field_name in sorted(referenced):
        base = _normalize_field(field_name)
        if base not in allowed:
            errors.append(
                f'Field `{field_name}` is not allowed on index `{index}`. '
                f"Allowed: {', '.join(sorted(allowed))}"
            )
    return errors


def validate_query(
    index: str,
    body: dict,
    *,
    log_rejection: bool = True,
) -> ValidatedQuery:
    """Validate index and search body. Raises ValidationError if unsafe or invalid."""
    errors: list[str] = []

    if index not in ALL_INDEX_NAMES:
        errors.append(
            f'Unknown index "{index}". Allowed: {", ".join(ALL_INDEX_NAMES)}'
        )

    if not isinstance(body, dict) or not body:
        errors.append('"body" must be a non-empty object')

    if errors:
        message = "; ".join(errors)
        if log_rejection:
            logger.warning("Query rejected: %s index=%s body=%s", message, index, body)
        raise ValidationError(message, errors=errors)

    errors.extend(_validate_top_level(body))
    errors.extend(_find_forbidden_keys(body))
    if "aggs" in body and isinstance(body["aggs"], dict):
        errors.extend(_validate_agg_types(body["aggs"]))
    errors.extend(_validate_fields_for_index(index, body))

    if errors:
        message = "; ".join(errors)
        if log_rejection:
            logger.warning("Query rejected: %s index=%s body=%s", message, index, body)
        raise ValidationError(message, errors=errors)

    return ValidatedQuery(index=index, body=body)
