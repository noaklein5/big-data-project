"""Create Elasticsearch indexes and mappings."""

from __future__ import annotations

from elasticsearch import Elasticsearch

from src.config import (
    ELASTICSEARCH_URL,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
)

INDEX_MAPPINGS = {
    INDEX_MOVIES: {
        "mappings": {
            "properties": {
                "movie_id": {"type": "integer"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "release_year": {"type": "integer"},
                "genres": {"type": "keyword"},
                "average_rating": {"type": "float"},
                "rating_count": {"type": "integer"},
                "tags": {"type": "keyword"},
                "tag_count": {"type": "integer"},
            }
        }
    },
    INDEX_MOVIES_BY_RELEASE_YEAR: {
        "mappings": {
            "properties": {
                "release_year": {"type": "integer"},
                "movie_count": {"type": "integer"},
                "total_rating_count": {"type": "integer"},
                "average_rating": {"type": "float"},
            }
        }
    },
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR: {
        "mappings": {
            "properties": {
                "movie_id": {"type": "integer"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}},
                },
                "rating_year": {"type": "integer"},
                "rating_count": {"type": "integer"},
                "average_rating": {"type": "float"},
            }
        }
    },
}


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


if __name__ == "__main__":
    create_indexes()
