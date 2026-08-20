# Stage 8 — Gold Query Catalog

Manual reference queries for AI evaluation (Stage 8 complete).

---

## Files

| File | Purpose |
|---|---|
| `catalog.yaml` | Question catalog (Step 1) |
| `queries.json` | Verified Elasticsearch DSL bodies (Step 2) |
| `../scripts/verify_gold_queries.py` | Run and verify all queries (Step 3) |

---

## Coverage (20 queries)

| Category | Index | Count |
|---|---|---|
| Filter + sort | `movies` | 7 |
| Aggregation | `movies` | 4 |
| Release year | `movies` | 2 |
| Release year cohort | `movies_by_release_year` | 3 |
| Rating activity | `movie_ratings_by_rating_year` | 4 |
| **Total** | | **20** |

---

## How to run and test

**Prerequisites:** Docker stack up + ETL data loaded.

### Run all gold queries

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py
```

Expected: `20/20 queries passed.`

### Run one query and see sample hits

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_02 --show-hits 3
```

### See full Elasticsearch response (useful for aggregations)

```powershell
docker exec movielens-app python scripts/verify_gold_queries.py --id movies_07 --show-response
```

### Kibana Dev Tools (manual)

1. Open http://localhost:5601 → **Dev Tools**
2. Copy the `"body"` from `queries.json` for any query
3. Run e.g. `GET movies/_search` + paste body

---

## Query IDs

| ID | Question (short) |
|---|---|
| `movies_01` | Comedy after 2000 |
| `movies_02` | Top 10 Comedy, ≥100 ratings |
| `movies_03` | pixar tag, avg > 3.5 |
| `movies_04` | Horror + funny tag |
| `movies_05` | 1990s Drama, ≥50 ratings |
| `movies_06` | Star Wars in title |
| `movies_07` | Top genres by avg rating |
| `movies_08` | Action vs Comedy avg |
| `movies_09` | Movies per decade |
| `movies_10` | Top rated released 2010 |
| `movies_11` | Busiest release years 1995–2005 |
| `cohort_01` | 1990s vs 2000s cohort avg |
| `cohort_02` | Top years by total ratings |
| `rating_year_01` | Most active movies in 2010 |
| `rating_year_02` | Toy Story activity over time |
| `rating_year_03` | Top rating years by volume |
| `movies_12` | Top 10 movies by rating count |
| `movies_13` | Animation + disney tag |
| `cohort_03` | Top 10 release years by movie count |
| `rating_year_04` | Most active movies in 2005 |

---

## Field semantics

| Field | Meaning | Indexes |
|---|---|---|
| `release_year` | When the movie was released | `movies`, `movies_by_release_year` |
| `rating_year` | When ratings were submitted | `movie_ratings_by_rating_year` |

Never use generic `year`.
