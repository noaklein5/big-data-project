"""Create Elasticsearch indexes and mappings."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from src.config import ELASTICSEARCH_URL
from src.elastic.schema import INDEX_MAPPINGS


def get_client() -> Elasticsearch:
    return Elasticsearch(ELASTICSEARCH_URL)


def create_indexes(client: Elasticsearch | None = None) -> None:
    es = client or get_client()
    for index_name, body in INDEX_MAPPINGS.items():
        if es.indices.exists(index=index_name):
            print(f"Index already exists: {index_name}")
            continue
        es.indices.create(index=index_name, mappings=body["mappings"])
        print(f"Created index: {index_name}")


def ensure_indexes(client: Elasticsearch | None = None) -> None:
    """Create indexes if missing — safe to call before ETL or verification."""
    create_indexes(client)


if __name__ == "__main__":
    create_indexes()
