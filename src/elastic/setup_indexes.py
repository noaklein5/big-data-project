"""Create Elasticsearch indexes and mappings."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from src.config import ELASTICSEARCH_URL
from src.elastic.schema import INDEX_MAPPINGS


def get_client() -> Elasticsearch:
    return Elasticsearch(ELASTICSEARCH_URL)


def _live_properties(client: Elasticsearch, index_name: str) -> dict:
    mapping = client.indices.get_mapping(index=index_name)
    return mapping[index_name]["mappings"].get("properties", {})


def _mapping_matches(client: Elasticsearch, index_name: str, expected: dict) -> bool:
    if not client.indices.exists(index=index_name):
        return False

    live_props = _live_properties(client, index_name)
    expected_props = expected["mappings"]["properties"]

    for field_name, spec in expected_props.items():
        live_field = live_props.get(field_name)
        if not live_field:
            return False
        if live_field.get("type") != spec.get("type"):
            return False
        if spec.get("type") == "text":
            keyword_field = live_field.get("fields", {}).get("keyword")
            expected_keyword = spec.get("fields", {}).get("keyword")
            if bool(keyword_field) != bool(expected_keyword):
                return False

    return True


def create_indexes(client: Elasticsearch | None = None, *, recreate_on_mismatch: bool = False) -> None:
    es = client or get_client()
    for index_name, body in INDEX_MAPPINGS.items():
        exists = es.indices.exists(index=index_name)
        if exists and not _mapping_matches(es, index_name, body):
            if recreate_on_mismatch:
                print(f"Recreating index (mapping mismatch): {index_name}")
                es.indices.delete(index=index_name)
                exists = False
            else:
                raise RuntimeError(
                    f"Index `{index_name}` exists with wrong mapping "
                    f"(often caused by Spark auto-creating the index). "
                    "Run: docker exec movielens-app python scripts/setup_infrastructure.py "
                    "after `docker compose down -v`, or recreate indexes before Spark ETL."
                )

        if exists:
            print(f"Index already exists: {index_name}")
            continue

        es.indices.create(index=index_name, mappings=body["mappings"])
        print(f"Created index: {index_name}")


def ensure_indexes(client: Elasticsearch | None = None, *, recreate_on_mismatch: bool = False) -> None:
    """Create indexes if missing — safe to call before ETL or verification."""
    create_indexes(client, recreate_on_mismatch=recreate_on_mismatch)


if __name__ == "__main__":
    create_indexes(recreate_on_mismatch=True)
