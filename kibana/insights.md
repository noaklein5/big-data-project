# MovieLens Kibana Insights (Stage 12)

Evidence-based observations generated from the loaded Elasticsearch indexes.

## Dataset snapshot

- **8,227** movie documents in `movies`
- **102** release-year cohorts in `movies_by_release_year`
- **36,447** movie–rating-year documents in `movie_ratings_by_rating_year`

## Key insights

1. **Genre catalog skew:** `Drama` has the most movies in the catalog (3,955 documents with that genre tag), so genre-based dashboards should treat it as the largest bucket.

2. **Release-year peak:** **2006** is the busiest release year in the cohort index (290 movies), useful when comparing production volume over time.

3. **Rating activity peak:** **2000** has the highest total rating activity (30,108 ratings in that calendar year), showing when users engaged most with the catalog.

4. **Quality vs. popularity:** Among genres with at least 100 rated movies, **Film-Noir** has the highest average rating (4.034), while the overall catalog average is **3.284**.

5. **Two different year fields:** `release_year` (when a movie came out) and `rating_year` (when ratings were submitted) answer different questions — the Kibana dashboard uses separate indexes so cohort and activity trends are not mixed.

## Dashboard charts

Open Kibana → **Dashboards** → **MovieLens Analytics** to explore:

- Average rating by genre
- Movies per genre
- Top movies by rating count
- Movies released per year
- Rating activity by year
- Cohort average rating by release year
