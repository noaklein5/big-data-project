"""Shared configuration loaded from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW_RATINGS = os.getenv("KAFKA_TOPIC_RAW_RATINGS", "raw_ratings")

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark:7077")

DATA_RAW_PATH = Path(os.getenv("DATA_RAW_PATH", "/data/raw"))
DATA_PROCESSED_PATH = Path(os.getenv("DATA_PROCESSED_PATH", "/data/processed"))

DATA_MODE = os.getenv("DATA_MODE", "sample")
SAMPLE_RATINGS = int(os.getenv("SAMPLE_RATINGS", "100000"))

STREAMLIT_PORT = int(os.getenv("STREAMLIT_PORT", "8501"))

INDEX_MOVIES = "movies"
INDEX_MOVIES_BY_RELEASE_YEAR = "movies_by_release_year"
INDEX_MOVIE_RATINGS_BY_RATING_YEAR = "movie_ratings_by_rating_year"
