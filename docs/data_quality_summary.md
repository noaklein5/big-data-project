# MovieLens 20M — Data Quality Summary (Stage 1)

Generated from full dataset analysis. See `notebooks/01_data_exploration.ipynb` for the interactive exploration.

---

## Dataset overview

| File | Rows | Notes |
|---|---:|---|
| `ratings.csv` | 20,000,263 | 138,493 users · 26,744 movies rated |
| `movies.csv` | 27,278 | Primary key: `movieId` |
| `tags.csv` | 465,564 | 38,643 unique tags (35,170 after lowercasing) |

---

## Ratings

| Check | Result |
|---|---|
| Missing values | 0 |
| Duplicate rows | 0 |
| Rating range | 0.5 – 5.0 |
| Half-star steps only | Yes |
| Timestamp range | 1995-01-09 → 2015-03-31 |
| `rating_year` range | 1995 – 2015 |

**Implication for ETL:** Derive `rating_year` from the rating `timestamp`. All ratings are valid and within expected bounds.

---

## Movies

| Check | Result |
|---|---|
| Missing values | 0 |
| Duplicate `movieId` | 0 |
| `(no genres listed)` | 246 movies |
| Titles with parsed `release_year` | 27,252 (99.9%) |
| Unparsed titles | 26 movies |

**Genre parsing:** Split `genres` on `|`. Top genres by movie count:

| Genre | Movies |
|---|---:|
| Drama | 13,344 |
| Comedy | 8,374 |
| Thriller | 4,178 |
| Romance | 4,127 |
| Action | 3,520 |
| Crime | 2,939 |
| Horror | 2,611 |
| Documentary | 2,471 |
| Adventure | 2,329 |
| Sci-Fi | 1,743 |

**Implication for ETL:** Parse `(YYYY)` from end of title for `release_year`. Handle `(no genres listed)` as empty genre array.

---

## Tags

| Check | Result |
|---|---|
| Missing values | 0 |
| Unique tags (raw) | 38,643 |
| Unique tags (lowercased) | 35,170 |
| Case normalization savings | 3,473 duplicate forms |

**Top tags (normalized):**

| Tag | Count |
|---|---:|
| sci-fi | 3,576 |
| based on a book | 3,307 |
| atmospheric | 3,169 |
| comedy | 3,078 |
| action | 3,068 |
| nudity (topless) | 2,646 |
| surreal | 2,528 |
| twist ending | 2,367 |
| bd-r | 2,334 |
| funny | 2,253 |

**Implication for ETL:** Lowercase and deduplicate tags per movie before indexing as keyword array.

---

## Cross-file integrity

| Check | Count |
|---|---:|
| Movies with no ratings | 534 |
| Movies with no tags | 7,733 |
| Ratings referencing unknown `movieId` | 0 |
| Tags referencing unknown `movieId` | 0 |

All `movieId` values in ratings and tags exist in `movies.csv`. Joins are clean.

534 movies in the catalog were never rated — expected for a large catalog. 7,733 movies have no user tags — tags are sparse compared to ratings.

---

## Recommendations for Stage 2 (schema lock)

1. Use **`release_year`** for when a movie was released (parsed from title).
2. Use **`rating_year`** for when a rating was submitted (from timestamp).
3. Store **`genres`** and **`tags`** as keyword arrays in Elasticsearch.
4. Validate ratings in ETL: keep only 0.5–5.0 in half-star steps.
5. Aggregate at movie level for the `movies` index; build cohort and time-series indexes as planned.

---

## Stage 1 status

- [x] Exploration notebook: `notebooks/01_data_exploration.ipynb`
- [x] Data quality summary: this document
- [ ] Team review before Stage 2 schema lock
