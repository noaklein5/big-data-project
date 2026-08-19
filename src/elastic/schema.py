"""Locked Elasticsearch data model — single source of truth for all tracks."""

from __future__ import annotations

from typing import Literal, TypedDict

from src.config import (
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
)

FieldRole = Literal["search", "filter", "sort", "agg"]


class FieldSpec(TypedDict):
    type: str
    roles: list[FieldRole]
    description: str


INDEX_FIELDS: dict[str, dict[str, FieldSpec]] = {
    INDEX_MOVIES: {
        "movie_id": {
            "type": "integer",
            "roles": ["filter"],
            "description": "Primary key; Elasticsearch document id",
        },
        "title": {
            "type": "text+keyword",
            "roles": ["search"],
            "description": "Movie title; use title.keyword for exact match",
        },
        "release_year": {
            "type": "integer",
            "roles": ["filter", "sort", "agg"],
            "description": "Year the movie was released (parsed from title)",
        },
        "genres": {
            "type": "keyword[]",
            "roles": ["filter", "agg"],
            "description": "Genre list parsed from pipe-delimited CSV field",
        },
        "average_rating": {
            "type": "float",
            "roles": ["filter", "sort", "agg"],
            "description": "All-time average rating for the movie",
        },
        "rating_count": {
            "type": "integer",
            "roles": ["filter", "sort", "agg"],
            "description": "Total number of ratings for the movie",
        },
        "tags": {
            "type": "keyword[]",
            "roles": ["filter", "search"],
            "description": "Normalized user tags aggregated per movie",
        },
        "tag_count": {
            "type": "integer",
            "roles": ["filter", "sort", "agg"],
            "description": "Number of distinct normalized tags for the movie",
        },
    },
    INDEX_MOVIES_BY_RELEASE_YEAR: {
        "release_year": {
            "type": "integer",
            "roles": ["filter", "sort", "agg"],
            "description": "Release year cohort key; Elasticsearch document id",
        },
        "movie_count": {
            "type": "integer",
            "roles": ["sort", "agg"],
            "description": "Number of movies released in this year",
        },
        "total_rating_count": {
            "type": "integer",
            "roles": ["sort", "agg"],
            "description": "Sum of rating_count across movies in the cohort",
        },
        "average_rating": {
            "type": "float",
            "roles": ["sort", "agg"],
            "description": "Weighted average rating for movies released in this year",
        },
    },
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR: {
        "movie_id": {
            "type": "integer",
            "roles": ["filter"],
            "description": "Movie primary key",
        },
        "title": {
            "type": "text+keyword",
            "roles": ["search"],
            "description": "Movie title copied for display in time-series results",
        },
        "rating_year": {
            "type": "integer",
            "roles": ["filter", "sort", "agg"],
            "description": "Calendar year the rating was submitted (from timestamp)",
        },
        "rating_count": {
            "type": "integer",
            "roles": ["filter", "sort", "agg"],
            "description": "Ratings submitted for this movie in this year",
        },
        "average_rating": {
            "type": "float",
            "roles": ["filter", "sort", "agg"],
            "description": "Average rating for this movie in this year",
        },
        "movie_rating_year_id": {
            "type": "keyword",
            "roles": ["filter"],
            "description": "Elasticsearch document id ({movie_id}_{rating_year})",
        },
    },
}

DOCUMENT_ID_FIELDS: dict[str, str | list[str]] = {
    INDEX_MOVIES: "movie_id",
    INDEX_MOVIES_BY_RELEASE_YEAR: "release_year",
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR: "movie_rating_year_id",
}

INDEX_MAPPINGS: dict[str, dict] = {
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
                "movie_rating_year_id": {"type": "keyword"},
            }
        }
    },
}

SPARK_WRITE_CONFIG = {
    INDEX_MOVIES: {
        "es.resource": INDEX_MOVIES,
        "es.mapping.id": "movie_id",
    },
    INDEX_MOVIES_BY_RELEASE_YEAR: {
        "es.resource": INDEX_MOVIES_BY_RELEASE_YEAR,
        "es.mapping.id": "release_year",
    },
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR: {
        "es.resource": INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
        "es.mapping.id": "movie_rating_year_id",
    },
}

FORBIDDEN_FIELDS = frozenset({"year"})

ALL_INDEX_NAMES = (
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
)


def get_field_names(index_name: str) -> frozenset[str]:
    return frozenset(INDEX_FIELDS[index_name].keys())


def get_allowed_fields(index_name: str) -> frozenset[str]:
    return get_field_names(index_name) - FORBIDDEN_FIELDS


def format_document_id(index_name: str, document: dict) -> str:
    id_spec = DOCUMENT_ID_FIELDS[index_name]
    if isinstance(id_spec, str):
        return str(document[id_spec])
    return "_".join(str(document[field]) for field in id_spec)


def get_llm_schema_prompt() -> str:
    lines = [
        "Use only the indexes and fields below.",
        "Never use a generic `year` field — use `release_year` or `rating_year`.",
        "",
    ]
    for index_name in ALL_INDEX_NAMES:
        lines.append(f"Index: {index_name}")
        for field_name, spec in INDEX_FIELDS[index_name].items():
            lines.append(f"  {field_name}: {spec['type']} — {spec['description']}")
        lines.append("")
    return "\n".join(lines).strip()
