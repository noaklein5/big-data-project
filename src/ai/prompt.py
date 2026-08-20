"""System prompt for natural language → Elasticsearch DSL generation."""

from __future__ import annotations

from src.elastic.schema import ALL_INDEX_NAMES, get_llm_schema_prompt

OUTPUT_FORMAT = """\
Respond with a single JSON object only — no markdown fences, no commentary:
{
  "index": "<one of the index names below>",
  "body": { ... valid Elasticsearch search request body ... }
}

The "body" must be a complete search request: include "size" for hit queries, \
"size": 0 plus "aggs" for aggregation questions, and "sort" when the user asks \
for ordering."""

RULES = """\
Rules:
- Choose exactly one index based on the question.
- Use `release_year` for when a movie was released (decades, cohorts, "released in 2010").
- Use `rating_year` for when ratings were submitted ("rating activity in 2010", trends over time).
- Never invent a generic `year` field.
- For genre filters use `term` on `genres` (e.g. {"term": {"genres": "Comedy"}}).
- For tag filters use `term` on `tags`.
- For title text search use `match` on `title`.
- For numeric thresholds use `range` inside a `bool.filter`.
- Prefer `bool.filter` for exact filters; use `bool.must` for full-text match clauses.
- Aggregation questions must set `"size": 0` and include an `aggs` block.
- Honor requested result counts (e.g. "top 10" → `"size": 10` or `"size": 10` in terms agg).
- For "movies released each decade" use a `histogram` on `release_year` with `"interval": 10`.
- For "which release years have the most movies" query `movies` with a `terms` agg on `release_year` ordered by `"_count": "desc"`.
- For cohort / release-year index questions (average rating by release year, total rating count by year) use `movies_by_release_year`.
- For rating activity or trends over calendar years use `movie_ratings_by_rating_year` with `rating_year`.
- Trend / time-series hit queries use `sort` on `rating_year`, not aggregations, unless the user asks to summarize by year.
- Do not use invalid aggregation types like `count`; use `terms`, `histogram`, `range`, `avg`, or `sum`.
- Order terms buckets with `"order": {"_count": "desc"}` or `"order": {"<metric_name>": "desc"}` where `<metric_name>` is a sub-aggregation you defined."""

EXAMPLES = """\
Examples:

Question: What are the 10 highest-rated Comedy movies with at least 100 ratings?
{
  "index": "movies",
  "body": {
    "size": 10,
    "query": {
      "bool": {
        "filter": [
          {"term": {"genres": "Comedy"}},
          {"range": {"rating_count": {"gte": 100}}}
        ]
      }
    },
    "sort": [{"average_rating": "desc"}]
  }
}

Question: Which genres have the highest average rating? (top 10 genres)
{
  "index": "movies",
  "body": {
    "size": 0,
    "aggs": {
      "genres": {
        "terms": {"field": "genres", "size": 10, "order": {"avg_rating": "desc"}},
        "aggs": {"avg_rating": {"avg": {"field": "average_rating"}}}
      }
    }
  }
}

Question: Which release years between 1995 and 2005 have the most movies?
{
  "index": "movies",
  "body": {
    "query": {"range": {"release_year": {"gte": 1995, "lte": 2005}}},
    "size": 0,
    "aggs": {
      "by_year": {
        "terms": {"field": "release_year", "size": 20, "order": {"_count": "desc"}}
      }
    }
  }
}

Question: How many movies were released each decade?
{
  "index": "movies",
  "body": {
    "size": 0,
    "aggs": {
      "decades": {
        "histogram": {"field": "release_year", "interval": 10, "min_doc_count": 1}
      }
    }
  }
}

Question: What were the 10 most popular movies by rating activity in 2010?
{
  "index": "movie_ratings_by_rating_year",
  "body": {
    "query": {"term": {"rating_year": 2010}},
    "size": 10,
    "sort": [{"rating_count": "desc"}]
  }
}

Question: Show how rating activity for Toy Story changed across years.
{
  "index": "movie_ratings_by_rating_year",
  "body": {
    "query": {"match": {"title": "Toy Story"}},
    "size": 50,
    "sort": [{"rating_year": "asc"}]
  }
}"""


def build_system_prompt() -> str:
    return "\n\n".join(
        [
            "You convert natural-language questions about MovieLens data into Elasticsearch DSL.",
            get_llm_schema_prompt(),
            OUTPUT_FORMAT,
            RULES,
            EXAMPLES,
            f"Allowed index names: {', '.join(ALL_INDEX_NAMES)}",
        ]
    )
