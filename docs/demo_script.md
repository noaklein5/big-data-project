# Demo Script (Stage 15)

Rehearsed live demo for the 5–10 minute presentation. All four questions **passed AI evaluation** on the full 20M load (see `docs/ai_evaluation.md`).

**Prerequisites**

```powershell
docker compose up -d
docker exec movielens-ollama ollama pull llama3.2:3b
# Pipeline already loaded (sample or full). For sample:
docker exec movielens-app python scripts/run_full_pipeline.py
```

**Open demo UI:** http://localhost:8501

**Fallback (if Streamlit slow):**

```powershell
docker exec movielens-app python scripts/run_ai_evaluation.py --id movies_02 --show-dsl
```

---

## Demo flow (~5 minutes)

| Step | What to say | What to show |
| --- | --- | --- |
| 1 | "We loaded MovieLens ratings through Kafka and Spark into three Elasticsearch indexes." | Kibana dashboard or `curl localhost:9200/movies/_count` |
| 2 | "Users ask questions in plain English; Ollama generates DSL; our validator checks it before search runs." | Streamlit NL search box |
| 3 | Run the four questions below | Results table in Streamlit |
| 4 | "We measured the AI on 20 gold questions — 75% fully correct index and results." | `docs/ai_evaluation.md` summary table |

---

## Question 1 — Filter + sort (`movies`)

**Ask:**

> What are the 10 highest-rated Comedy movies with at least 100 ratings?

**Gold ID:** `movies_02`

**Expected:** Results from `movies` index; Comedy genre filter; sorted by average rating; well-known titles (e.g. high-rated comedies with large rating counts).

**Verify beforehand:**

```powershell
docker exec movielens-app python scripts/run_ai_evaluation.py --id movies_02
```

---

## Question 2 — Rating activity by year (`movie_ratings_by_rating_year`)

**Ask:**

> What were the 10 most popular movies by rating activity in 2010?

**Gold ID:** `rating_year_01`

**Expected:** Hits from `movie_ratings_by_rating_year`; filter `rating_year=2010`; sorted by rating count descending.

**Verify beforehand:**

```powershell
docker exec movielens-app python scripts/run_ai_evaluation.py --id rating_year_01
```

---

## Question 3 — Tag filter (`movies`)

**Ask:**

> Show movies tagged "pixar" with an average rating above 3.5.

**Gold ID:** `movies_03`

**Expected:** Small result set from `movies` (Pixar-related tags); average rating filter applied.

**Verify beforehand:**

```powershell
docker exec movielens-app python scripts/run_ai_evaluation.py --id movies_03
```

---

## Question 4 — Genre aggregation (`movies`)

**Ask:**

> Compare the average rating of Action movies vs Comedy movies.

**Gold ID:** `movies_08`

**Expected:** Aggregation on `movies` index; buckets or comparison for Action and Comedy genres.

**Verify beforehand:**

```powershell
docker exec movielens-app python scripts/run_ai_evaluation.py --id movies_08
```

---

## Optional backup questions

If the model fails on a live question, switch to these (also passed evaluation):

| Question | Gold ID |
| --- | --- |
| Which genres have the highest average rating? (top 10) | `movies_07` |
| Which 10 release years have the most movies? | `cohort_03` |

---

## Kibana segment (optional, +1 min)

1. Open http://localhost:5601
2. **Dashboards → MovieLens Analytics**
3. Point out: genre distribution, rating activity by year, cohort chart
4. Mention insight: **2000** had peak rating activity; **Film-Noir** highest avg among major genres (`kibana/insights.md`)

---

## Troubleshooting during demo

| Issue | Fix |
| --- | --- |
| Ollama timeout | `docker exec movielens-ollama ollama list` — ensure `llama3.2:3b` pulled |
| Empty results | Confirm ES has data: `docker exec movielens-app python scripts/verify_integration.py --mode auto` |
| Invalid query error | Validator blocked bad DSL — rephrase or use backup question |
| Streamlit not loading | `docker compose restart app` |
