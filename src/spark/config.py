"""Spark job configuration (env vars with Docker defaults)."""

from __future__ import annotations

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW_RATINGS = os.getenv("KAFKA_TOPIC_RAW_RATINGS", "raw_ratings")

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")

DATA_RAW_PATH = os.getenv("DATA_RAW_PATH", "/data/raw")

INDEX_MOVIES = "movies"
INDEX_MOVIES_BY_RELEASE_YEAR = "movies_by_release_year"
INDEX_MOVIE_RATINGS_BY_RATING_YEAR = "movie_ratings_by_rating_year"
