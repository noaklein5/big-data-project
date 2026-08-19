# MovieLens Smart Analytics — Locked Data Model (Stage 2)

This document is the **team synchronization point** for Spark, Elasticsearch, Kibana, and the AI layer.

Based on Stage 1 findings (`docs/data_quality_summary.md`).

---

## Design principles

| Rule | Decision |
|---|---|
| Year semantics | `release_year` = when the movie came out · `rating_year` = when ratings were submitted |
| Forbidden field | Never use generic `year` |
| Primary join key | `movie_id` (from CSV `movieId`) |
| Genres | Parse `genres` on `\|`; `(no genres listed)` → empty array |
| Tags | Lowercase, trim, deduplicate per movie before indexing |
| Ratings | Keep 0.5–5.0 in half-star steps only |
| Document IDs | Explicit on every Elasticsearch write |

---

## Index overview

| Index | Granularity | Document ID | Purpose |
|---|---|---|---|
| `movies` | 1 doc / movie | `movie_id` | Main search index — filters, sorts, genre/tag queries |
| `movies_by_release_year` | 1 doc / release year | `release_year` | Cohort rollups and decade comparisons |
| `movie_ratings_by_rating_year` | 1 doc / (movie, rating year) | `{movie_id}_{rating_year}` | Rating activity trends over time |

---

## Index 1: `movies`

All-time aggregated stats per movie.

### Example document

```json
{
  "movie_id": 1,
  "title": "Toy Story (1995)",
  "release_year": 1995,
  "genres": ["Adventure", "Animation", "Children", "Comedy", "Fantasy"],
  "average_rating": 3.92,
  "rating_count": 49695,
  "tags": ["pixar", "animation", "funny"],
  "tag_count": 1520
}
```

### Fields

| Field | ES type | Search | Filter | Sort | Agg | Notes |
|---|---|:---:|:---:|:---:|:---:|---|
| `movie_id` | integer | | ✓ | | | Document ID |
| `title` | text + keyword | ✓ | | | | Full-text search; `title.keyword` for exact |
| `release_year` | integer | | ✓ | ✓ | ✓ | Parsed from `(YYYY)` in title |
| `genres` | keyword | | ✓ | | ✓ | Multi-value; term filter per genre |
| `average_rating` | float | | ✓ | ✓ | ✓ | Mean of all ratings |
| `rating_count` | integer | | ✓ | ✓ | ✓ | Count of ratings |
| `tags` | keyword | ✓ | ✓ | | | Normalized tag list |
| `tag_count` | integer | | ✓ | ✓ | ✓ | Distinct tag count |

### Spark aggregation

```text
ratings ──join── movies ──left join── aggregated tags
        │
        └── groupBy movie_id → average_rating, rating_count
                              + title, release_year, genres, tags, tag_count
```

### Example questions → this index

- Show Comedy movies released after 2000
- Top 10 highest-rated Comedy movies with at least 5,000 ratings
- Movies tagged "dark" with average rating above 4
- Which genres have the highest average rating?

---

## Index 2: `movies_by_release_year`

Cohort-level rollups by release year.

### Example document

```json
{
  "release_year": 2010,
  "movie_count": 842,
  "total_rating_count": 1250000,
  "average_rating": 3.45
}
```

### Fields

| Field | ES type | Filter | Sort | Agg | Notes |
|---|---|:---:|:---:|:---:|---|
| `release_year` | integer | ✓ | ✓ | ✓ | Document ID |
| `movie_count` | integer | | ✓ | ✓ | Movies released that year |
| `total_rating_count` | integer | | ✓ | ✓ | Sum of ratings across cohort |
| `average_rating` | float | | ✓ | ✓ | Weighted by rating_count |

### Spark aggregation

```text
movies (with release_year) joined with movie-level rating stats
        │
        └── groupBy release_year → movie_count, total_rating_count, average_rating
```

### Example questions → this index

- Compare average ratings of movies released in the 1990s vs the 2000s
- Which release years have the most movies?

For **highest-rated movies released in 2010**, query `movies` with `release_year: 2010`.

---

## Index 3: `movie_ratings_by_rating_year`

Rating activity per movie per calendar year (from rating timestamp).

### Example document

```json
{
  "movie_id": 1,
  "title": "Toy Story (1995)",
  "rating_year": 2010,
  "rating_count": 1450,
  "average_rating": 4.02
}
```

### Fields

| Field | ES type | Search | Filter | Sort | Agg | Notes |
|---|---|:---:|:---:|:---:|:---:|---|
| `movie_id` | integer | | ✓ | | | Part of composite document ID |
| `title` | text + keyword | ✓ | | | | Denormalized for display |
| `rating_year` | integer | | ✓ | ✓ | ✓ | From rating `timestamp`, not release year |
| `rating_count` | integer | | ✓ | ✓ | ✓ | Ratings in that year |
| `average_rating` | float | | ✓ | ✓ | ✓ | Mean rating in that year |

### Spark aggregation

```text
ratings (with rating_year derived from timestamp)
        │
        └── groupBy (movie_id, rating_year) → rating_count, average_rating
             + join title from movies
```

### Example questions → this index

- Most popular movies by rating activity in 2010
- How did rating activity for a movie change over time?

---

## Raw CSV → schema mapping

| Raw source | Raw field | Output field | Transform |
|---|---|---|---|
| `movies.csv` | `movieId` | `movie_id` | Rename |
| `movies.csv` | `title` | `title` | As-is |
| `movies.csv` | `title` | `release_year` | Regex `\((\d{4})\)$` |
| `movies.csv` | `genres` | `genres` | Split `\|`, skip `(no genres listed)` |
| `ratings.csv` | `movieId` | `movie_id` | Rename |
| `ratings.csv` | `rating` | — | Validate 0.5–5.0 |
| `ratings.csv` | `timestamp` | `rating_year` | `year(timestamp)` |
| `tags.csv` | `movieId` | `movie_id` | Rename |
| `tags.csv` | `tag` | `tags[]` | lower + trim + dedupe per movie |

---

## Spark → Elasticsearch direct write

| Index | Connector setting | Document ID |
|---|---|---|
| `movies` | `es.resource=movies` | `movie_id` |
| `movies_by_release_year` | `es.resource=movies_by_release_year` | `release_year` |
| `movie_ratings_by_rating_year` | `es.resource=movie_ratings_by_rating_year` | `{movie_id}_{rating_year}` |

Package: `org.elasticsearch:elasticsearch-spark-30_2.12` (match Spark 3.5 / Scala 2.12).

---

## Kafka message contract (ratings stream)

Topic: `raw_ratings`

```json
{
  "userId": 23,
  "movieId": 356,
  "rating": 4.5,
  "timestamp": 123456789
}
```

`movies.csv` and `tags.csv` are static Spark inputs (volume mount), not streamed.

---

## Code references

| Component | Location |
|---|---|
| Schema constants + LLM prompt | `src/elastic/schema.py` |
| Index creation | `src/elastic/setup_indexes.py` |
| Index names | `src/config.py` |
| Stage 1 data findings | `docs/data_quality_summary.md` |

---

## Stage 2 checklist

- [x] Three indexes defined with field names and types
- [x] `release_year` vs `rating_year` semantics locked
- [x] Searchable vs filter/sort/agg roles documented
- [x] Spark output → index mapping defined
- [x] Elasticsearch document IDs defined
- [x] Shared schema module for ES + AI + validator
- [ ] Team sign-off
