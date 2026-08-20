# Kibana — Stage 12

Analytics dashboards for the MovieLens Elasticsearch indexes.

## Setup

```powershell
docker exec movielens-app python scripts/setup_kibana.py
docker exec movielens-app python scripts/verify_kibana.py
```

## Open the dashboard

1. Go to http://localhost:5601
2. **Analytics** → **Dashboards**
3. Open **MovieLens Analytics**

Direct link (after setup):

http://localhost:5601/app/dashboards#/view/movielens-analytics

## Data views

| ID | Index |
|---|---|
| `dv-movies` | `movies` |
| `dv-release-year` | `movies_by_release_year` |
| `dv-rating-year` | `movie_ratings_by_rating_year` |

## Charts (6)

1. Average rating by genre
2. Movies per genre
3. Top movies by rating count
4. Movies released per year
5. Rating activity by year
6. Cohort average rating by release year

## Insights

After setup, see `kibana/insights.md` for evidence-based observations generated from live Elasticsearch data.

Re-run setup to refresh insights if ETL data changes.
