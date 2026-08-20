"""Generate evidence-based insights from Elasticsearch for Stage 12."""

from __future__ import annotations

from pathlib import Path

from elasticsearch import Elasticsearch

from src.config import (
    ELASTICSEARCH_URL,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
)

INSIGHTS_PATH = Path(__file__).resolve().parent.parent.parent / "kibana" / "insights.md"


def _top_genre_by_movie_count(client: Elasticsearch) -> tuple[str, int]:
    response = client.search(
        index=INDEX_MOVIES,
        size=0,
        aggs={
            "genres": {
                "terms": {"field": "genres", "size": 1, "order": {"_count": "desc"}},
            }
        },
    )
    bucket = response["aggregations"]["genres"]["buckets"][0]
    return bucket["key"], bucket["doc_count"]


def _top_release_year_by_movie_count(client: Elasticsearch) -> tuple[int, int]:
    response = client.search(
        index=INDEX_MOVIES_BY_RELEASE_YEAR,
        size=1,
        sort=[{"movie_count": "desc"}],
        _source=["release_year", "movie_count"],
    )
    source = response["hits"]["hits"][0]["_source"]
    return int(source["release_year"]), int(source["movie_count"])


def _top_rating_year_by_activity(client: Elasticsearch) -> tuple[int, int]:
    response = client.search(
        index=INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
        size=0,
        aggs={
            "years": {
                "terms": {"field": "rating_year", "size": 1, "order": {"total": "desc"}},
                "aggs": {"total": {"sum": {"field": "rating_count"}}},
            }
        },
    )
    bucket = response["aggregations"]["years"]["buckets"][0]
    return int(bucket["key"]), int(bucket["total"]["value"])


def _highest_rated_genre(client: Elasticsearch, min_movies: int = 100) -> tuple[str, float]:
    response = client.search(
        index=INDEX_MOVIES,
        size=0,
        query={"range": {"rating_count": {"gte": min_movies}}},
        aggs={
            "genres": {
                "terms": {"field": "genres", "size": 1, "order": {"avg_rating": "desc"}},
                "aggs": {"avg_rating": {"avg": {"field": "average_rating"}}},
            }
        },
    )
    bucket = response["aggregations"]["genres"]["buckets"][0]
    return bucket["key"], round(bucket["avg_rating"]["value"], 3)


def _average_ratings_per_movie(client: Elasticsearch) -> float:
    response = client.search(
        index=INDEX_MOVIES,
        size=0,
        aggs={"avg_rating": {"avg": {"field": "average_rating"}}},
    )
    return round(response["aggregations"]["avg_rating"]["value"], 3)


def generate_insights(client: Elasticsearch | None = None) -> str:
    es = client or Elasticsearch(ELASTICSEARCH_URL)

    movies_count = es.count(index=INDEX_MOVIES)["count"]
    release_years_count = es.count(index=INDEX_MOVIES_BY_RELEASE_YEAR)["count"]
    rating_year_docs = es.count(index=INDEX_MOVIE_RATINGS_BY_RATING_YEAR)["count"]

    top_genre, genre_movie_count = _top_genre_by_movie_count(es)
    top_release_year, release_year_movies = _top_release_year_by_movie_count(es)
    top_rating_year, rating_activity = _top_rating_year_by_activity(es)
    best_genre, best_genre_rating = _highest_rated_genre(es)
    overall_avg = _average_ratings_per_movie(es)

    lines = [
        "# MovieLens Kibana Insights (Stage 12)",
        "",
        "Evidence-based observations generated from the loaded Elasticsearch indexes.",
        "",
        "## Dataset snapshot",
        "",
        f"- **{movies_count:,}** movie documents in `{INDEX_MOVIES}`",
        f"- **{release_years_count:,}** release-year cohorts in `{INDEX_MOVIES_BY_RELEASE_YEAR}`",
        f"- **{rating_year_docs:,}** movie–rating-year documents in `{INDEX_MOVIE_RATINGS_BY_RATING_YEAR}`",
        "",
        "## Key insights",
        "",
        f"1. **Genre catalog skew:** `{top_genre}` has the most movies in the catalog "
        f"({genre_movie_count:,} documents with that genre tag), so genre-based dashboards "
        "should treat it as the largest bucket.",
        "",
        f"2. **Release-year peak:** **{top_release_year}** is the busiest release year in the cohort index "
        f"({release_year_movies:,} movies), useful when comparing production volume over time.",
        "",
        f"3. **Rating activity peak:** **{top_rating_year}** has the highest total rating activity "
        f"({rating_activity:,} ratings in that calendar year), showing when users engaged most with the catalog.",
        "",
        f"4. **Quality vs. popularity:** Among genres with at least 100 rated movies, "
        f"**{best_genre}** has the highest average rating ({best_genre_rating:.3f}), "
        f"while the overall catalog average is **{overall_avg:.3f}**.",
        "",
        "5. **Two different year fields:** `release_year` (when a movie came out) and `rating_year` "
        "(when ratings were submitted) answer different questions — the Kibana dashboard uses separate "
        "indexes so cohort and activity trends are not mixed.",
        "",
        "## Dashboard charts",
        "",
        "Open Kibana → **Dashboards** → **MovieLens Analytics** to explore:",
        "",
        "- Average rating by genre",
        "- Movies per genre",
        "- Top movies by rating count",
        "- Movies released per year",
        "- Rating activity by year",
        "- Cohort average rating by release year",
        "",
    ]
    return "\n".join(lines)


def write_insights(path: Path | None = None, client: Elasticsearch | None = None) -> Path:
    target = path or INSIGHTS_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(generate_insights(client=client), encoding="utf-8")
    return target
