"""Stage 4 sample ETL — 100k ratings → three Elasticsearch indexes."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from src.config import (
    DATA_MODE,
    DATA_PROCESSED_PATH,
    DATA_RAW_PATH,
    ELASTICSEARCH_URL,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    SAMPLE_RATINGS,
)
from src.elastic.schema import format_document_id
from src.etl.transforms import (
    extract_release_year,
    is_valid_rating,
    normalize_tag,
    parse_genres,
    timestamp_to_rating_year,
)


@dataclass(frozen=True)
class PipelineStats:
    ratings_loaded: int
    ratings_valid: int
    movies_in_sample: int
    movies_index_docs: int
    release_year_index_docs: int
    rating_year_index_docs: int


def _read_ratings_sample(limit: int | None) -> pd.DataFrame:
    path = DATA_RAW_PATH / "ratings.csv"
    ratings = pd.read_csv(path, nrows=limit)
    ratings = ratings.rename(columns={"movieId": "movie_id"})
    return ratings


def _read_movies() -> pd.DataFrame:
    path = DATA_RAW_PATH / "movies.csv"
    movies = pd.read_csv(path)
    return movies.rename(columns={"movieId": "movie_id"})


def _read_tags() -> pd.DataFrame:
    path = DATA_RAW_PATH / "tags.csv"
    tags = pd.read_csv(path)
    return tags.rename(columns={"movieId": "movie_id"})


def _validate_ratings(ratings: pd.DataFrame) -> pd.DataFrame:
    valid_mask = ratings["rating"].map(is_valid_rating)
    invalid_count = int((~valid_mask).sum())
    if invalid_count:
        print(f"Dropping {invalid_count:,} ratings outside 0.5–5.0 half-star range")
    cleaned = ratings.loc[valid_mask].copy()
    cleaned["rating_year"] = cleaned["timestamp"].map(timestamp_to_rating_year)
    return cleaned


def _prepare_movies(movies: pd.DataFrame) -> pd.DataFrame:
    prepared = movies.copy()
    prepared["release_year"] = prepared["title"].map(extract_release_year)
    prepared["genres"] = prepared["genres"].map(parse_genres)
    return prepared


def _aggregate_tags(tags: pd.DataFrame, movie_ids: set[int]) -> pd.DataFrame:
    filtered = tags.loc[tags["movie_id"].isin(movie_ids)].copy()
    filtered["tag"] = filtered["tag"].map(normalize_tag)
    filtered = filtered.loc[filtered["tag"].astype(bool)]

    grouped = (
        filtered.groupby("movie_id")["tag"]
        .agg(lambda values: sorted(set(values)))
        .reset_index()
    )
    grouped["tag_count"] = grouped["tag"].map(len)
    grouped = grouped.rename(columns={"tag": "tags"})
    return grouped


def build_movies_documents(
    ratings: pd.DataFrame,
    movies: pd.DataFrame,
    tags_by_movie: pd.DataFrame,
) -> list[dict]:
    rating_stats = (
        ratings.groupby("movie_id")["rating"]
        .agg(average_rating="mean", rating_count="count")
        .reset_index()
    )

    sample_movies = movies.merge(rating_stats, on="movie_id", how="inner")
    sample_movies = sample_movies.merge(tags_by_movie, on="movie_id", how="left")
    sample_movies["tags"] = sample_movies["tags"].apply(
        lambda value: value if isinstance(value, list) else []
    )
    sample_movies["tag_count"] = sample_movies["tag_count"].fillna(0).astype(int)
    sample_movies["average_rating"] = sample_movies["average_rating"].round(4)

    documents: list[dict] = []
    for row in sample_movies.itertuples(index=False):
        doc = {
            "movie_id": int(row.movie_id),
            "title": row.title,
            "genres": list(row.genres),
            "average_rating": float(row.average_rating),
            "rating_count": int(row.rating_count),
            "tags": list(row.tags),
            "tag_count": int(row.tag_count),
        }
        if pd.notna(row.release_year):
            doc["release_year"] = int(row.release_year)
        documents.append(doc)
    return documents


def build_release_year_documents(movies_documents: list[dict]) -> list[dict]:
    frame = pd.DataFrame(movies_documents)
    frame = frame.loc[frame["release_year"].notna()].copy()
    if frame.empty:
        return []

    grouped = frame.groupby("release_year", as_index=False).agg(
        movie_count=("movie_id", "count"),
        total_rating_count=("rating_count", "sum"),
    )
    weighted = frame.copy()
    weighted["weighted_sum"] = weighted["average_rating"] * weighted["rating_count"]
    weighted_avg = (
        weighted.groupby("release_year", as_index=False)
        .agg(weighted_sum=("weighted_sum", "sum"), rating_count=("rating_count", "sum"))
        .assign(
            average_rating=lambda df: (df["weighted_sum"] / df["rating_count"]).round(4)
        )[["release_year", "average_rating"]]
    )
    cohort = grouped.merge(weighted_avg, on="release_year")

    return [
        {
            "release_year": int(row.release_year),
            "movie_count": int(row.movie_count),
            "total_rating_count": int(row.total_rating_count),
            "average_rating": float(row.average_rating),
        }
        for row in cohort.itertuples(index=False)
    ]


def build_rating_year_documents(
    ratings: pd.DataFrame,
    movies: pd.DataFrame,
) -> list[dict]:
    grouped = (
        ratings.groupby(["movie_id", "rating_year"])["rating"]
        .agg(average_rating="mean", rating_count="count")
        .reset_index()
    )
    titles = movies[["movie_id", "title"]]
    joined = grouped.merge(titles, on="movie_id", how="left")
    joined["average_rating"] = joined["average_rating"].round(4)

    return [
        {
            "movie_id": int(row.movie_id),
            "title": row.title,
            "rating_year": int(row.rating_year),
            "rating_count": int(row.rating_count),
            "average_rating": float(row.average_rating),
        }
        for row in joined.itertuples(index=False)
    ]


def _bulk_index(
    client: Elasticsearch,
    index_name: str,
    documents: list[dict],
) -> None:
    actions = (
        {
            "_index": index_name,
            "_id": format_document_id(index_name, document),
            "_source": document,
        }
        for document in documents
    )
    success, errors = bulk(client, actions, raise_on_error=False)
    if errors:
        raise RuntimeError(f"Bulk index to {index_name} failed: {errors[:3]}")
    print(f"Indexed {success:,} documents into {index_name}")


def _save_processed_outputs(
    movies_documents: list[dict],
    release_year_documents: list[dict],
    rating_year_documents: list[dict],
) -> None:
    DATA_PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(movies_documents).to_parquet(
        DATA_PROCESSED_PATH / "sample_movies.parquet",
        index=False,
    )
    pd.DataFrame(release_year_documents).to_parquet(
        DATA_PROCESSED_PATH / "sample_movies_by_release_year.parquet",
        index=False,
    )
    pd.DataFrame(rating_year_documents).to_parquet(
        DATA_PROCESSED_PATH / "sample_movie_ratings_by_rating_year.parquet",
        index=False,
    )
    print(f"Saved processed parquet files under {DATA_PROCESSED_PATH}")


def run_sample_etl(
    *,
    write_elasticsearch: bool = True,
    save_parquet: bool = True,
    sample_size: int | None = None,
) -> PipelineStats:
    limit = sample_size
    if limit is None and DATA_MODE == "sample":
        limit = SAMPLE_RATINGS

    print(f"Loading ratings sample (limit={limit or 'all'}) from {DATA_RAW_PATH}")
    ratings_raw = _read_ratings_sample(limit)
    ratings = _validate_ratings(ratings_raw)

    movies = _prepare_movies(_read_movies())
    movie_ids = set(ratings["movie_id"].unique())
    movies = movies.loc[movies["movie_id"].isin(movie_ids)]

    print(f"Aggregating tags for {len(movie_ids):,} movies in sample")
    tags_by_movie = _aggregate_tags(_read_tags(), movie_ids)

    movies_documents = build_movies_documents(ratings, movies, tags_by_movie)
    release_year_documents = build_release_year_documents(movies_documents)
    rating_year_documents = build_rating_year_documents(ratings, movies)

    stats = PipelineStats(
        ratings_loaded=len(ratings_raw),
        ratings_valid=len(ratings),
        movies_in_sample=len(movie_ids),
        movies_index_docs=len(movies_documents),
        release_year_index_docs=len(release_year_documents),
        rating_year_index_docs=len(rating_year_documents),
    )

    print(
        f"Built documents: movies={stats.movies_index_docs:,}, "
        f"release_year={stats.release_year_index_docs:,}, "
        f"rating_year={stats.rating_year_index_docs:,}"
    )

    if save_parquet:
        _save_processed_outputs(
            movies_documents,
            release_year_documents,
            rating_year_documents,
        )

    if write_elasticsearch:
        client = Elasticsearch(ELASTICSEARCH_URL)
        _bulk_index(client, INDEX_MOVIES, movies_documents)
        _bulk_index(client, INDEX_MOVIES_BY_RELEASE_YEAR, release_year_documents)
        _bulk_index(client, INDEX_MOVIE_RATINGS_BY_RATING_YEAR, rating_year_documents)

    return stats
