# AI Evaluation Report (Stage 14)

Generated: 2026-08-20 12:47 UTC
Model: `llama3.2:3b`
Questions evaluated: 20
Runtime: 161.5s

## Summary metrics

| Metric | Rate |
| --- | --- |
| Valid query rate | 95.0% |
| Index selection accuracy | 95.0% |
| Correct-result rate | 75.0% |
| Semantic correctness rate | 75.0% |

Semantic correctness = correct index **and** results meet gold thresholds (min hits / aggregation buckets).

## Results by question

| ID | Category | Valid? | Index OK? | Results OK? | Semantic OK? | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| movies_01 | filter_sort | yes | yes | yes | yes | index=movies, hits=3,826, agg=0 |
| movies_02 | filter_sort | yes | yes | yes | yes | index=movies, hits=3,107, agg=0 |
| movies_03 | filter_sort | yes | yes | yes | yes | index=movies, hits=41, agg=0 |
| movies_04 | filter_sort | yes | yes | yes | yes | index=movies, hits=0, agg=0 |
| movies_05 | filter_sort | yes | yes | yes | yes | index=movies, hits=5,157, agg=0 |
| movies_06 | filter_sort | yes | yes | yes | yes | index=movies, hits=63, agg=0 |
| movies_07 | aggregation | yes | yes | yes | yes | index=movies, hits=10,000, agg=10 |
| movies_08 | aggregation | yes | yes | yes | yes | index=movies, hits=10,000, agg=10 |
| movies_09 | aggregation | yes | yes | yes | yes | index=movies, hits=10,000, agg=13 |
| movies_10 | release_year | no | no | no | no | Field `rating_count` is not allowed on index `movies_by_release_year`. Allowe... |
| movies_11 | release_year | yes | yes | yes | yes | index=movies, hits=6,581, agg=11 |
| cohort_01 | release_year_cohort | yes | yes | no | no | Elasticsearch error: BadRequestError(400, 'search_phase_execution_exception',... |
| cohort_02 | release_year_cohort | yes | yes | no | no | Elasticsearch error: BadRequestError(400, 'search_phase_execution_exception',... |
| rating_year_01 | rating_activity | yes | yes | yes | yes | index=movie_ratings_by_rating_year, hits=10,000, agg=0 |
| rating_year_02 | rating_activity | yes | yes | yes | yes | index=movie_ratings_by_rating_year, hits=1,398, agg=0 |
| rating_year_03 | aggregation | yes | yes | yes | yes | index=movie_ratings_by_rating_year, hits=10,000, agg=5 |
| movies_12 | aggregation | yes | yes | no | no | Elasticsearch error: BadRequestError(400, 'x_content_parse_exception', '[term... |
| movies_13 | filter_sort | yes | yes | no | no | Elasticsearch error: BadRequestError(400, 'parsing_exception', '[bool] malfor... |
| cohort_03 | release_year_cohort | yes | yes | yes | yes | index=movies_by_release_year, hits=118, agg=10 |
| rating_year_04 | rating_activity | yes | yes | yes | yes | index=movie_ratings_by_rating_year, hits=8,509, agg=0 |

## Question details

### movies_01

**Question:** Show Comedy movies released after 2000.

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 3,826
- Aggregation buckets: 0

### movies_02

**Question:** What are the 10 highest-rated Comedy movies with at least 100 ratings?

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 3,107
- Aggregation buckets: 0

### movies_03

**Question:** Show movies tagged "pixar" with an average rating above 3.5.

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 41
- Aggregation buckets: 0

### movies_04

**Question:** Show Horror movies tagged "funny" sorted by average rating descending.

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 0
- Aggregation buckets: 0

### movies_05

**Question:** List Drama movies from the 1990s with at least 50 ratings, sorted by rating count.

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 5,157
- Aggregation buckets: 0

### movies_06

**Question:** Find movies with "Star Wars" in the title and at least 10 ratings.

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 63
- Aggregation buckets: 0

### movies_07

**Question:** Which genres have the highest average rating? (top 10 genres)

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 10,000
- Aggregation buckets: 10

### movies_08

**Question:** Compare the average rating of Action movies vs Comedy movies.

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 10,000
- Aggregation buckets: 10

### movies_09

**Question:** How many movies were released each decade?

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 10,000
- Aggregation buckets: 13

### movies_10

**Question:** What are the 10 highest-rated movies released in 2010 with at least 5 ratings?

**Status:** FAIL

- Expected index: `movies`
- Error: Field `rating_count` is not allowed on index `movies_by_release_year`. Allowed: average_rating, movie_count, release_year, total_rating_count

### movies_11

**Question:** Which release years between 1995 and 2005 have the most movies?

**Status:** OK

- Expected index: `movies`
- Generated index: `movies`
- Hits: 6,581
- Aggregation buckets: 11

### cohort_01

**Question:** Compare average ratings of movies released in the 1990s vs the 2000s.

**Status:** FAIL

- Expected index: `movies_by_release_year`
- Generated index: `movies_by_release_year`
- Error: Elasticsearch error: BadRequestError(400, 'search_phase_execution_exception', 'Invalid aggregation order path [avg_rating]. The provided aggregation [avg_rating] either does not exist, or is a pipeline aggregation and cannot be used to sort the buckets.')

### cohort_02

**Question:** Which release years have the highest total rating count? (top 10)

**Status:** FAIL

- Expected index: `movies_by_release_year`
- Generated index: `movies_by_release_year`
- Error: Elasticsearch error: BadRequestError(400, 'search_phase_execution_exception', 'Invalid aggregation order path [total_rating_count]. The provided aggregation [total_rating_count] either does not exist, or is a pipeline aggregation and cannot be used to sort the buckets.')

### rating_year_01

**Question:** What were the 10 most popular movies by rating activity in 2010?

**Status:** OK

- Expected index: `movie_ratings_by_rating_year`
- Generated index: `movie_ratings_by_rating_year`
- Hits: 10,000
- Aggregation buckets: 0

### rating_year_02

**Question:** Show how rating activity for Toy Story changed across years.

**Status:** OK

- Expected index: `movie_ratings_by_rating_year`
- Generated index: `movie_ratings_by_rating_year`
- Hits: 1,398
- Aggregation buckets: 0

### rating_year_03

**Question:** Which rating years had the most total ratings submitted? (top 5)

**Status:** OK

- Expected index: `movie_ratings_by_rating_year`
- Generated index: `movie_ratings_by_rating_year`
- Hits: 10,000
- Aggregation buckets: 5

### movies_12

**Question:** What are the 10 movies with the highest rating count?

**Status:** FAIL

- Expected index: `movies`
- Generated index: `movies`
- Error: Elasticsearch error: BadRequestError(400, 'x_content_parse_exception', '[term] query does not support [gte]', [term] query does not support [gte])

### movies_13

**Question:** Show Animation movies tagged "disney" sorted by average rating.

**Status:** FAIL

- Expected index: `movies`
- Generated index: `movies`
- Error: Elasticsearch error: BadRequestError(400, 'parsing_exception', '[bool] malformed query, expected [END_OBJECT] but found [FIELD_NAME]')

### cohort_03

**Question:** Which 10 release years have the most movies?

**Status:** OK

- Expected index: `movies_by_release_year`
- Generated index: `movies_by_release_year`
- Hits: 118
- Aggregation buckets: 10

### rating_year_04

**Question:** What were the 10 most popular movies by rating activity in 2005?

**Status:** OK

- Expected index: `movie_ratings_by_rating_year`
- Generated index: `movie_ratings_by_rating_year`
- Hits: 8,509
- Aggregation buckets: 0
