# MovieLens Smart Analytics — תוכנית פרויקט

## מטרת הפרויקט

לבנות מערכת Big Data על גבי מערך הנתונים **MovieLens 20M**, שמאפשרת למשתמשים לחקור דירוגי סרטים, ז'אנרים, תגיות, פופולריות ומגמות באמצעות **שאלות בשפה טבעית**.

צינור ה-Big Data יעבד את נתוני MovieLens באמצעות Kafka, Spark ו-Elasticsearch.  
רכיב ה-AI יממש **שפה טבעית → שאילתת Elasticsearch**:

```text
User Question
    ↓
LLM (Ollama)
    ↓
Elasticsearch DSL Query
    ↓
Validation
    ↓
Elasticsearch
    ↓
Real Results from MovieLens
```

ה-LLM מייצר את השאילתה, אך התשובה הסופית מבוססת על תוצאות אמיתיות שחוזרות ממערך הנתונים.

---

## אילוצי הפרויקט

| אילוץ | החלטה |
|---|---|
| גודל צוות | עד 3 חברים |
| לוח זמנים | גמיש — קודם דגימה, אחר כך מערך מלא |
| פריסה (Deployment) | הדגמה מקומית על לפטופ בלבד — ללא פריסה בענן |
| ניהול גרסאות | מאגר Git משותף לצוות |
| תשתית | כל הרכיבים רצים ב-Docker דרך `docker compose` |
| טביעת Docker | מינימום מספר containers וגודל image ללפטופ יחיד (~8–12 GB RAM) |
| LLM | Ollama עם מודל קוד פתוח קטן (`llama3.2:3b` מומלץ) |
| פלט Spark | Spark כותב **ישירות** ל-Elasticsearch (מחבר elasticsearch-spark) |

### נתונים חצי-מובנים / לא מובנים (דרישת הקורס)

MovieLens הוא בעיקר CSV, אך הפרויקט עומד בדרישת הקורס באמצעות:

- **Tags** — טקסט חופשי שנוצר על ידי משתמשים, מנורמל ומצטבר לכל סרט
- **Genres** — שדה חצי-מובנה מופרד ב-`|`, שמפורש למערכים
- **Titles** — כותרות לא מובנות, שמפורשות לחילוץ שנת יציאה

יש לתעד זאת במפורש במסמך העיצוב.

---

# ארכיטקטורה כללית

```text
MovieLens 20M
ratings.csv + movies.csv + tags.csv
             │
             ▼
           Kafka
             │
             ▼
           Spark
      Cleaning + Joins
      + Aggregations
             │
             ▼ (direct write)
       Elasticsearch
             │
      ┌──────┴──────┐
      ▼             ▼
 Analytics        AI Layer
 / Kibana        User Question
                     ↓
                 Ollama (LLM)
                     ↓
              Elasticsearch DSL
                     ↓
                Validation
                     ↓
              Elasticsearch
                     ↓
                Real Results
```

כל השירותים רצים מקומית ב-Docker. נתונים גולמיים נטענים כ-volume מ-`./data/raw`.

---

# שלבי הפרויקט

## שלב 0 — מאגר הפרויקט וסביבת פיתוח

**סטטוס נוכחי:** הושלם (מבנה repo, stack של Docker, config).

### משימות
- יצירת מאגר Git משותף.
- הגדרת מבנה תיקיות הפרויקט.
- יצירה של:
  - `requirements.txt`
  - `.gitignore` (לכלול `data/raw/`, `.env`, `.venv/`)
  - `.env.example` (תבנית — לא לבצע commit לסודות)
  - `README.md` ראשוני
- יצירת `docker-compose.yml` עם stack מקומי מלא (ראו שלב 3).
- הורדת MovieLens 20M מקומית ל-`data/raw/`.
- שמירת מערך הנתונים הגולמי הגדול מחוץ ל-Git.
- אימות ש-`docker compose up` מפעיל את כל השירותים על לפטופ אחד.

### מבנה פרויקט מוצע

```text
movielens-bigdata-ai/
│
├── data/
│   ├── raw/              # MovieLens 20M — לא ב-Git
│   └── processed/
│
├── notebooks/
│   └── 01_data_exploration.ipynb
│
├── src/
│   ├── producer/
│   ├── spark/
│   ├── elastic/
│   ├── ai/
│   └── app/
│
├── tests/
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

### תוצר סופי
כל חברי הצוות יכולים לבצע clone למאגר, לטעון את מערך הנתונים ולהריץ `docker compose up`.

### עבודה מקבילית
לא. לבצע פעם אחת כצוות.

---

## שלב 1 — חקירה והבנה של MovieLens

התמקדות בשלושה קבצים:

### `ratings.csv`
```text
userId
movieId
rating
timestamp
```

### `movies.csv`
```text
movieId
title
genres
```

### `tags.csv`
```text
userId
movieId
tag
timestamp
```

### משימות

#### ניתוח ratings
- מספר שורות.
- מספר משתמשים.
- מספר סרטים.
- ערכים חסרים.
- שורות כפולות.
- התפלגות דירוגים.
- אימות טווח דירוגים.
- טווח timestamps.
- דירוגים לכל סרט.
- דירוגים לכל משתמש.
- גזירת `rating_year` מ-`timestamp` לניתוח מגמות לאורך זמן.

#### ניתוח movies
- מספר סרטים.
- ערכים חסרים.
- `movieId` כפול.
- פירוק שנת סרט מהכותרת, ככל האפשר.
- פירוק `genres` המופרד ב-`|`.
- ספירת סרטים לפי ז'אנר.
- בדיקת סרטים עם `(no genres listed)`.

#### ניתוח tags
- מספר רשומות tag.
- מספר tags ייחודיים.
- ערכים חסרים.
- tags כפולים.
- נרמול רישיות לחקירה.
- tags נפוצים ביותר.
- tags לכל סרט.
- טווח timestamps.

#### בדיקות בין-קבציות
- אימות ש-`movieId` מתחבר כראוי.
- סרטים ללא דירוגים.
- סרטים ללא tags.
- דירוגים שמצביעים על סרטים לא קיימים.
- tags שמצביעים על סרטים לא קיימים.

### תוצר סופי
מחברת חקירת נתונים וסיכום קצר של איכות הנתונים.

### עבודה מקבילית
כן.

חלוקה מוצעת (עד 3 חברים):
- **חבר A:** `ratings.csv`
- **חבר B:** `movies.csv`
- **חבר C:** `tags.csv`

לאחר מכן לשלב מסקנות יחד.

---

## שלב 2 — הגדרת מודל הנתונים הסופי

שלב זה חייב להיות מוסכם על ידי כל הצוות לפני שהענפים המימושיים מתפצלים.

### אינדקס 1: `movies` (ראשי)

מסמך אחד לכל סרט עם סטטיסטיקות מצטברות לכל הזמנים.

```json
{
  "movie_id": 1,
  "title": "Toy Story",
  "release_year": 1995,
  "genres": ["Adventure", "Animation", "Children", "Comedy", "Fantasy"],
  "average_rating": 3.92,
  "rating_count": 49695,
  "tags": ["pixar", "animation", "funny"],
  "tag_count": 1520
}
```

תומך בשאלות כגון:

> הצג סרטי Comedy שיצאו אחרי 2000.

> מהם 10 סרטי ה-Comedy בעלי הדירוג הגבוה ביותר עם לפחות 5,000 דירוגים?

### אינדקס 2: `movies_by_release_year` (קבוצות לפי שנת יציאה)

מסמך אחד לכל שנת יציאה עם צבירות ברמת הקohort.

```json
{
  "release_year": 2010,
  "movie_count": 842,
  "total_rating_count": 1250000,
  "average_rating": 3.45
}
```

תומך בשאלות כגון:

> השווה דירוגים ממוצעים של סרטים שיצאו בשנות ה-90 לעומת שנות ה-2000.

> באילו שנות יציאה יש הכי הרבה סרטים?

> מהם הסרטים בעלי הדירוג הגבוה ביותר ש**יצאו** ב-2010?

לשאלה האחרונה, יש לשאול את אינדקס `movies` עם `release_year: 2010` ולמיין לפי `average_rating` או `rating_count`. אינדקס הקohort תומך בהשוואות וצבירות ברמת שנה.

### אינדקס 3: `movie_ratings_by_rating_year` (פעילות דירוג לאורך זמן)

מסמך אחד לכל `(movie_id, rating_year)` — `rating_year` נגזר מ-`timestamp` של הדירוג, **לא** משנת יציאת הסרט.

```json
{
  "movie_id": 1,
  "title": "Toy Story",
  "rating_year": 2010,
  "rating_count": 1450,
  "average_rating": 4.02
}
```

תומך בשאלות כגון:

> מהם הסרטים הפופולריים ביותר **ב-2010** (לפי פעילות דירוג באותה שנה)?

> כיצד השתנתה פעילות הדירוג של Toy Story לאורך הזמן?

### כללי שמות שדות (חשוב ל-LLM)

| שדה | משמעות | בשימוש ב- |
|---|---|---|
| `release_year` | השנה שבה הסרט יצא | `movies`, `movies_by_release_year` |
| `rating_year` | שנת לוח השנה שבה הוגש הדירוג | `movie_ratings_by_rating_year` |

לעולם לא להשתמש בשדה גנרי `year` — תמיד `release_year` או `rating_year`.

### להחליט יחד
- שמות שדות וסוגי נתונים מדויקים (נעולים כמפורט לעיל).
- שדות שניתן לחפש בהם לעומת שדות לצבירה בלבד.
- אילו פלטי Spark ממופים לאיזה אינדקס.
- אילו שאלות בשפה טבעית המערכת חייבת לתמוך בהן.
- תצורת כתיבה ישירה Spark → Elasticsearch (שמות אינדקסים, שדות id).

### תוצר סופי
סכמה מתועדת המשותפת ל-Spark, Elasticsearch ושכבת ה-AI.

### עבודה מקבילית
לא. זהו נקודת סנכרון לכל הצוות.

---

## שלב 3 — Docker ותשתית

כל הרכיבים רצים ב-Docker על לפטופ יחיד. יש למזער מספר containers ושימוש בזיכרון.

### Stack יעד (~5–6 containers)

| שירות | Image / הערות | רמז לזיכרון |
|---|---|---|
| **kafka** | Apache Kafka, מצב KRaft (ללא Zookeeper) | ~512 MB |
| **elasticsearch** | צומת יחיד, `discovery.type=single-node` | 512 MB–1 GB heap |
| **kibana** | תואם לגרסת ES | ~512 MB |
| **spark** | Apache Spark — 1 master + 1 worker | 1–2 GB |
| **ollama** | תמונת Ollama רשמית | 2–4 GB (תלוי במודל) |
| **app** | Python — producer, ממשק Streamlit, לקוח AI, validator | ~512 MB |

### בחירות עיצוב להקטנת טביעה
- Kafka KRaft במקום Kafka + Zookeeper (−1 container).
- צומת Elasticsearch יחיד עם heap מוגבל.
- container **app** מאוחד אחד במקום שירותים נפרדים ל-producer/UI/AI.
- נתוני MovieLens גולמיים נטענים כ-volume (`./data/raw:/data`).
- משיכת מודל Ollama קטן אחד: `llama3.2:3b`.

### רכיבים
- Kafka (KRaft)
- Elasticsearch
- Kibana
- Spark
- Ollama
- App (Python)

### משימות
- יצירת `docker-compose.yml` עם כל השירותים לעיל.
- הגדרת רשת Docker משותפת ו-volume mounts.
- הגדרת מגבלות זיכרון במידת האפשר (`ES_JAVA_OPTS`, זיכרון worker של Spark).
- הפעלת כל השירותים ואימות קישוריות.
- יצירת Kafka topic: `raw_ratings`.
- אימות תקינות Elasticsearch (`/_cluster/health`).
- אימות ש-Kibana מתחבר ל-Elasticsearch.
- משיכת מודל Ollama: `docker exec ollama ollama pull llama3.2:3b`.
- תיעוד סדר הפעלה ופורטים צפויים ב-`README.md`.

### תוצר סופי
`docker compose up` מפעיל את stack המקומי המלא על לפטופ אחד.

### עבודה מקבילית
כן. ניתן לבצע במקביל לשלב 4 לאחר שהסכמה מוסכמת.

---

## שלב 4 — בניית אב-טיפוס ETL קטן

**אין** להתחיל מיד עם כל 20 מיליון הדירוגים.

יש להשתמש בדגימה, לדוגמה:

```text
100,000 ratings
```

### משימות
- טעינת דגימה קטנה.
- אימות ערכי דירוג.
- המרת timestamps וגזירת `rating_year`.
- Join של ratings עם movies.
- פירוק genres ושנת יציאה מהכותרת.
- צבירת דירוגים לפי סרט.
- צבירת tags.
- חישוב ברמת סרט: `average_rating`, `rating_count`, `tag_count`.
- חישוב סטטיסטיקות cohort לפי שנת יציאה עבור `movies_by_release_year`.
- חישוב סטטיסטיקות `(movie_id, rating_year)` עבור `movie_ratings_by_rating_year`.
- כתיבת פלט הדגימה ישירות ל-Elasticsearch (אימות הגדרת המחבר).

### תוצר סופי
מערך נתונים מעובד קטן שנטען לכל שלושת אינדקסי Elasticsearch, בהתאם לסכמת שלב 2.

### עבודה מקבילית
כן. ניתן לבצע במקביל לשלב 3.

---

## שלב 5 — Kafka Producer

שימוש ב-Kafka בעיקר עבור זרם הדירוגים הגדול.

### עיצוב מוצע

```text
ratings.csv → Kafka
movies.csv  → Spark static input (volume mount)
tags.csv    → Spark static input (volume mount)
```

זה מדגים **נתוני streaming + נתוני ייחוס batch/סטטיים** ללא מורכבות מיותרת.

ה-producer רץ בתוך container ה-**app** (או מופעל ממנו).

### Kafka topic
```text
raw_ratings
```

### דוגמת הודעת Kafka

```json
{
  "userId": 23,
  "movieId": 356,
  "rating": 4.5,
  "timestamp": 123456789
}
```

### משימות
- בניית Kafka producer ב-Python ב-`src/producer/`.
- קריאת `ratings.csv` מה-volume שנטען.
- המרת כל שורה ל-JSON.
- שליחת רשומות ל-`raw_ratings`.
- הוספת מצבי **sample / full** הניתנים להגדרה דרך משתנה סביבה.
- אימות שניתן לצרוך הודעות.

### תוצר סופי
אירועי דירוג אמיתיים מ-MovieLens נכנסים ל-Kafka.

### עבודה מקבילית
כן. ניתן לחפוף לשלב 6 לאחר שהסכמה וחוזה Kafka קבועים.

---

## שלב 6 — צינור Spark ETL

Spark מבצע את העיבוד המשמעותי וכותב **ישירות** ל-Elasticsearch באמצעות **מחבר elasticsearch-spark**.

### זרימה מוצעת

```text
Read ratings from Kafka
        ↓
Validate rating range
        ↓
Convert timestamp → derive rating_year
        ↓
Read movies.csv (static)
        ↓
Parse title / release_year / genres
        ↓
Read tags.csv (static)
        ↓
Normalize and aggregate tags
        ↓
Join datasets
        ↓
Aggregate by movie → write to index: movies
        ↓
Aggregate by release_year → write to index: movies_by_release_year
        ↓
Aggregate by (movie_id, rating_year) → write to index: movie_ratings_by_rating_year
```

### כתיבה ישירה Spark → Elasticsearch

- שימוש ב-`org.elasticsearch:elasticsearch-spark-30_2.12` (התאמה לגרסאות Spark/Scala).
- הגדרת host, port ושמות אינדקס של ES דרך config של Spark או סביבת `docker-compose`.
- הגדרת document IDs במפורש (`movie_id` עבור `movies`; מפתח מורכב עבור `movie_ratings_by_rating_year`).
- בדיקה עם דגימת שלב 4 לפני הרצת מערך 20M המלא.

### משימות
- הגדרת מקור Kafka ב-Spark.
- פירוק הודעות JSON מ-Kafka.
- אימות וניקוי שדות.
- טעינת נתוני movie/tag סטטיים מ-volume mount.
- ביצוע joins.
- פירוק genres וחילוץ `release_year` מכותרות.
- נרמול tags (אותיות קטנות, deduplicate).
- חישוב שלושת פלטי הצבירה.
- כתיבת כל פלט ישירות לאינדקס Elasticsearch שלו.
- טיפול ברשומות חסרות/לא תקינות.
- תמיכה במצב sample/full בהתאמה ל-producer של Kafka.

### תוצר סופי
צינור ETL מלא מייצר נתונים נקיים ומצטברים שכתובים ישירות ל-Elasticsearch.

### עבודה מקבילית
חלקית. producer של Kafka ו-Spark ETL יכולים להיפתח על ידי חברים שונים לאחר שמוסכם הממשק ביניהם.

---

## שלב 7 — אינדקסים ו-Mappings של Elasticsearch

יצירת כל שלושת האינדקסים:

```text
movies
movies_by_release_year
movie_ratings_by_rating_year
```

### מapping לעיון

#### `movies`
```text
movie_id         integer
title            text + keyword subfield
genres           keyword
release_year     integer
average_rating   float
rating_count     integer
tags             keyword
tag_count        integer
```

#### `movies_by_release_year`
```text
release_year         integer
movie_count          integer
total_rating_count   integer
average_rating       float
```

#### `movie_ratings_by_rating_year`
```text
movie_id         integer
title            text + keyword subfield
rating_year      integer
rating_count     integer
average_rating   float
```

### משימות
- הגדרת mappings לכל שלושת האינדקסים.
- יצירת אינדקסים (דרך סקריפט setup ב-`src/elastic/` או כתיבה מ-Spark עם רמזי mapping).
- טעינת נתוני דגימה משלב 4.
- אימות מסננים exact-match (`genres`, `tags`, `release_year`, `rating_year`).
- אימות טווחים מספריים ומיון.
- אימות aggregations (ממוצעים לפי ז'אנר, cohorts לפי שנת יציאה, מגמות לפי שנת דירוג).
- אימות חיפוש tags.

### תוצר סופי
Elasticsearch מכיל נתוני MovieLens הניתנים לשאילתה בכל שלושת האינדקסים.

### עבודה מקבילית
כן. ניתן לפתח mappings באמצעות פלט דגימה משלב 4 בזמן השלמת Spark.

---

## שלב 8 — בניית שאילתות Elasticsearch ידניות

לפני שימוש ב-LLM, יש ליצור ידנית את השאילתות שהמערכת צפויה לייצר.

אלו הופכות ל**שאילתות זהב/ייחוס** לבדיקת ה-AI.

יש לכלול שאילתות ל**כל שלושת האינדקסים** וגם דפוסי **filter/sort** וגם **aggregation**.

### שאלות לדוגמה

#### מסננים ומיון (אינדקס `movies`)
> הצג סרטי Comedy שיצאו אחרי 2000.

> מהם 10 סרטי ה-Comedy בעלי הדירוג הגבוה ביותר עם לפחות 5,000 דירוגים?

> הצג סרטים מתויגים "dark" עם דירוג ממוצע מעל 4.

> הצג סרטי Horror בעלי דירוג גבוה שמתויגים "funny".

#### Aggregations (אינדקס `movies`)
> לאילו ז'אנרים יש את הדירוג הממוצע הגבוה ביותר?

> השווה דירוגים ממוצעים של סרטי Action לעומת Comedy.

#### שנת יציאה (`movies` או `movies_by_release_year`)
> מהם הסרטים בעלי הדירוג הגבוה ביותר ש**יצאו** ב-2010?

> לאילו עשורים של שנות יציאה יש את הדירוג הממוצע הגבוה ביותר?

#### פעילות דירוג לאורך זמן (`movie_ratings_by_rating_year`)
> מהם הסרטים הפופולריים ביותר **לפי פעילות דירוג** ב-2010?

> הצג כיצד השתנתה פעילות הדירוג של סרט מסוים לאורך השנים.

### משימות
- הגדרת 15–20 שאלות יעד בשפה טבעית שמכסות את כל הקטגוריות לעיל.
- כתיבת Elasticsearch DSL הנכון ידנית לכל אחת.
- ציון לאיזה אינדקס כל שאילתה מיועדת.
- אימות כל שאילתה מול Elasticsearch.
- שמירת מקרים ב-`tests/gold_queries/` להערכה מאוחר יותר.

### תוצר סופי
ערכת שאילתות שנבדקה, בלתי תלויה ב-LLM.

### עבודה מקבילית
כן. ניתן לבצע בזמן השלמת אינטגרציית Spark ו-Elasticsearch.

---

## שלב 9 — AI: שפה טבעית → שאילתת Elasticsearch

זוהי יכולת ה-AI המדורגת.

### הגדרת LLM
- **Runtime:** Ollama ב-Docker
- **Model:** `llama3.2:3b` (קטן, רץ על לפטופ)
- **Endpoint:** `http://ollama:11434` (מתוך container ה-app)

### קלט ל-LLM

#### שאלת משתמש
```text
What are the 10 highest-rated Comedy movies
with at least 5,000 ratings?
```

#### סכמת Elasticsearch (לספק את כל שלושת האינדקסים)
```text
Index: movies
  title: text
  genres: keyword[]
  release_year: integer
  average_rating: float
  rating_count: integer
  tags: keyword[]
  tag_count: integer

Index: movies_by_release_year
  release_year: integer
  movie_count: integer
  total_rating_count: integer
  average_rating: float

Index: movie_ratings_by_rating_year
  movie_id: integer
  title: text
  rating_year: integer   ← year the rating was submitted, NOT release year
  rating_count: integer
  average_rating: float
```

#### הוראות ל-LLM
- לייצר Elasticsearch DSL JSON בלבד.
- לבחור את האינדקס הנכון לפי השאלה.
- להשתמש ב-`release_year` לשנת יציאת הסרט.
- להשתמש ב-`rating_year` לשנת הגשת הדירוגים.
- לכלול `aggs` לשאלות aggregation (ז'אנרים, עשורים, השוואות).

### פלט לדוגמה (filter + sort על `movies`)

```json
{
  "size": 10,
  "query": {
    "bool": {
      "filter": [
        { "term": { "genres": "Comedy" } },
        { "range": { "rating_count": { "gte": 5000 } } }
      ]
    }
  },
  "sort": [{ "average_rating": "desc" }]
}
```

### פלט לדוגמה (aggregation על `movies`)

```json
{
  "size": 0,
  "aggs": {
    "by_genre": {
      "terms": { "field": "genres", "size": 20 },
      "aggs": {
        "avg_rating": { "avg": { "field": "average_rating" } }
      }
    }
  }
}
```

### משימות
- אינטגרציה של לקוח Ollama ב-`src/ai/`.
- הגדרת system prompt עם סכמה מלאה וסמנטיקת שדות.
- הגבלת הפלט ל-JSON/DSL בלבד.
- פירוק פלט המודל (הסרת markdown fences אם קיימים).
- טיפול בפלט פגום בצורה אלגנטית.
- בדיקה מול ערכת שאילתות הזהב של שלב 8.

### תוצר סופי
שאלות בשפה טבעית מייצרות באופן אמין שאילתות Elasticsearch מול האינדקס הנכון.

### עבודה מקבילית
כן. עיצוב prompt ושאילתות זהב יכולים להתחיל לפני סיום צינור מערך הנתונים המלא.

---

## שלב 10 — Query Validator

לעולם לא לבצע פלט LLM באופן עיוור.

### זרימת אימות

```text
LLM output
    ↓
Valid JSON?
    ↓
Allowed Elasticsearch structure?
    ↓
Uses only existing fields on the target index?
    ↓
Read-only query?
    ↓
Reasonable result size?
    ↓
Execute query
```

### משימות
- אימות תחביר JSON.
- לאפשר רק פעולות search/query (`query`, `aggs`, `sort`, `size`, `_source`).
- לדחות פעולות update/delete/ניהול אינדקס.
- לאמת שמות שדות מול הסכמה של האינדקס היעד.
- לדחות שאילתות שמשתמשות ב-`year` — לדרוש `release_year` או `rating_year`.
- להחיל מגבלות גודל תוצאה (למשל `size` ≤ 100).
- להחזיר שגיאות שימושיות לממשק.
- לרשום שאילתות שנדחו לצורך בדיקות.

### תוצר סופי
רק שאילתות search בטוחות ותקינות מגיעות ל-Elasticsearch.

### עבודה מקבילית
כן. ניתן לפתח במקביל לשלב 9.

---

## שלב 11 — אפליקציית הדגמה

לשמור על ממשק פשוט. רץ מקומית ב-container ה-**app** (Streamlit).

### ממשק מוצע

```text
┌──────────────────────────────────────────────┐
│ MovieLens Smart Analytics                   │
│                                              │
│ Ask a question about MovieLens:             │
│                                              │
│ [ Which Comedy movies have the highest... ] │
│                                              │
│                   [ Search ]                 │
└──────────────────────────────────────────────┘
```

### תצוגה
1. שאלת המשתמש.
2. אינדקס יעד (מזוהה או מוסק).
3. שאילתת Elasticsearch שנוצרה.
4. תוצאות אמיתיות שחזרו.
5. הסבר קצר אופציונלי (מסומן בבירור כמיוצר על ידי LLM).

### משימות
- בניית ממשק Streamlit ב-`src/app/`.
- חיבור ל-Ollama ו-Elasticsearch דרך רשת Docker.
- הצגת DSL שנוצר לפני ביצוע.
- ביצוע שאילתה מאומתת.
- הצגת תוצאות בבהירות (טבלה ל-hits, JSON ל-aggregations).
- טיפול בשגיאות (כשל LLM, שאילתה לא תקינה, timeout של ES).

### תוצר סופי
ממשק הדגמה מקומי עובד — ללא צורך בפריסה.

### עבודה מקבילית
כן. ניתן לרוץ במקביל לשלב 12.

---

## שלב 12 — Kibana ותובנות נתונים

יצירת dashboard קטן שמראה שצינור ה-Big Data מייצר תוצאות אנליטיות שימושיות.

### אפשרויות גрафיות
- הסרטים המדורגים ביותר.
- דירוג ממוצע לפי ז'אנר.
- התפלגות דירוגים.
- דירוגים לאורך זמן (`rating_year` מ-`movie_ratings_by_rating_year`).
- סרטים לפי שנת יציאה (`movies_by_release_year`).
- tags נפוצים ביותר.
- דירוג ממוצע מול מספר דירוגים.

### משימות
- יצירת data views ב-Kibana לכל שלושת האינדקסים.
- בניית 4–6 גרפים שימושיים.
- חילוץ 3–5 תובנות משמעותיות מהנתונים האמיתיים.

### תוצר סופי
Dashboard ומספר תובנות מבוססות-ראיות למצגת.

### עבודה מקבילית
כן. במקביל מלא לעבודת demo/UI.

---

## שלב 13 — אינטגרציה מלאה

להפסיק לעבוד בנפרד ולחבר הכל.

### זרימה מקצה לקצה

```text
docker compose up
        ↓
Kafka + Spark + Elasticsearch + Ollama + App
        ↓
Producer (sample or full mode)
        ↓
Spark ETL → direct write to Elasticsearch
        ↓
Streamlit App
```

### בדיקת זרימת AI

```text
User Question
        ↓
Ollama (llama3.2:3b)
        ↓
Elasticsearch DSL
        ↓
Validator
        ↓
Elasticsearch
        ↓
Real Results
```

### משימות
- הרצת stack מלא על לפטופ אחד.
- בדיקת צינור מנתונים גולמיים דרך כל שלושת אינדקסי ES.
- מעבר מדגימה ל**מערך 20M המלא**.
- בדיקת כל רכיבי ה-AI מול האינדקסים הסופיים.
- תיקון אי-התאמות סכמה בין פלט Spark ל-mappings של ES.
- בדיקת התנהגות restart (`docker compose down && docker compose up`).
- תיעוד סדר הפעלה ומצב sample/full ב-`README.md`.

### תוצר סופי
מערכת שלמה ועובדת שרצה מקומית.

### עבודה מקבילית
לא. זהו נקודת סנכרון לכל הצוות.

---

## שלב 14 — הערכת רכיב ה-AI

הכנת כ-**20 שאלות בשפה טבעית**.

קטגוריות מוצעות:

```text
5 simple filters        (movies index)
5 aggregations          (movies index)
5 release_year questions (movies / movies_by_release_year)
5 rating_year / tag questions (movie_ratings_by_rating_year / movies)
```

### טבלת הערכה

| שאלה | אינדקס | שאילתה תקינה? | נכונה סמנטית? | תוצאה נכונה? |
|---|---|---:|---:|---:|
| Top Comedy movies | movies | ✅ | ✅ | ✅ |
| Movies tagged dark | movies | ✅ | ✅ | ✅ |
| Genre avg rating | movies | ✅ | ✅ | ✅ |
| Popular by rating activity in 2010 | movie_ratings_by_rating_year | ✅ | ✅ | ✅ |
| Best movies released in 2010 | movies | ✅ | ✅ | ✅ |
| ... | ... | ... | ... | ... |

### מדדים
```text
Valid query rate
Semantic correctness rate
Correct-result rate
Index selection accuracy (correct index chosen)
```

אין להמציא את האחוזים הסופיים. יש לדווח על תוצאות הבדיקה בפועל.

### תוצר סופי
ראיה שרכיב ה-AI נבדק, ולא רק הודגם על דוגמאות שנבחרו בקפידה.

### עבודה מקבילית
כן. ניתן לחלק שאלות הערכה ותוצאות בין חברי הצוות.

---

## שלב 15 — תוצרים ומצגת

### תוצרים נדרשים

#### קוד מקור
- מאגר Git (או ייצוא ZIP).
- מבנה פרויקט נקי.
- `README.md` עם הוראות `docker compose up`.
- `.env.example` המתעד את כל משתני התצורה.

#### מסמך עיצוב (1–2 עמודים)
- בעיה ומטרה.
- מערך נתונים (קישור ל-MovieLens 20M).
- דיאגרמת ארכיטקטורה.
- זרימת נתונים (Kafka → Spark → Elasticsearch → Ollama → App).
- הצדקה לנתונים חצי-מובנים/לא מובנים (tags, genres, titles).
- טכנולוגיות ומדוע כל אחת נבחרה.
- יכולת AI (NL → ES DSL עם validation).
- פשרות עיקריות (streaming מדומה, ES צומת יחיד, LLM קטן).

#### מצגת (5–10 דקות)
1. בעיה ומטרה.
2. מערך נתונים MovieLens.
3. ארכיטקטורה.
4. צינור Big Data.
5. טרנספורמציות Spark וכתיבה ישירה ל-ES.
6. מודל נתונים Elasticsearch (שלושה אינדקסים, סמנטיקת שדות).
7. AI של שפה טבעית → שאילתה (Ollama).
8. הדגמה.
9. הערכה/תוצאות.
10. אתגרים ופשרות.

#### הדגמה
הכנת 3–4 שאלות אמינות מראש:

- סרטי Comedy בעלי הדירוג הגבוה ביותר עם מספיק דירוגים (`movies`).
- הסרטים הפופולריים ביותר לפי פעילות דירוג בשנה מסוימת (`movie_ratings_by_rating_year`).
- סרטים המשויכים ל-tag מסוים (`movies`).
- השוואת ז'אנר או שאלת cohort לפי שנת יציאה (`movies` / `movies_by_release_year`).

### תוצר סופי
כל דרישות ההגשה הושלמו.

### עבודה מקבילית
כן.

חלוקה מוצעת:
- חבר אחד: README והוראות הרצה.
- חבר אחד: מסמך ארכיטקטורה/עיצוב.
- חבר אחד: הכנת מצגת/הדגמה.

סקירה סופית צריכה להיעשות על ידי כל הצוות.

---

# חלוקת צוות מומלצת (עד 3 חברים)

| חבר | אחריות ראשית | אחריות משנית |
|---|---|---|
| **A — Data** | Kafka + Spark ETL + כתיבה ישירה ל-ES | חקירת נתונים |
| **B — Search** | mappings של Elasticsearch + Kibana | שאילתות ידניות/ייחוס |
| **C — AI/App** | Ollama + Validator + ממשק Streamlit | הערכת AI |

אם לצוות פחות מ-3 חברים, יש לשלב מסלולים סמוכים (למשל A לוקח Data + תשתית Search, C לוקח AI/App).

---

# תוכנית פיתוח מקבילית

## שלב 1 — יחד

```text
Stage 0 — Environment + Docker stack
Stage 1 — Dataset exploration
Stage 2 — Final data model (3 indexes, field semantics)
```

כולם צריכים להבין ולהסכים על אלה.

## שלב 2 — מסלולים מקבילים

```text
                     AGREED DATA MODEL
                            │
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
     MEMBER A           MEMBER B           MEMBER C
        DATA               SEARCH              AI
          │                 │                 │
       Kafka             ES mappings       Gold queries
       Spark             Kibana            Ollama prompt
       ES direct write  Manual queries    Validator + UI
          │                 │                 │
          └─────────────────┼─────────────────┘
                            ▼
                       INTEGRATION
```

### מסלול A — Data
- Kafka producer (מצב sample/full).
- צינור Spark ETL.
- כתיבה ישירה לכל שלושת אינדקסי ES.

### מסלול B — Search
- mappings ו-setup של Elasticsearch.
- טעינת דגימה ואימות שאילתות.
- dashboard ב-Kibana.

### מסלול C — AI
- שאלות בשפה טבעית ו-DSL ייחוס.
- אינטגרציה של Ollama (`llama3.2:3b`).
- Query validator.
- ממשק demo ב-Streamlit.

## שלב 3 — יחד

```text
Stage 13 — Integration (full dataset)
Stage 14 — Evaluation
Stage 15 — Final review and demo
```

כל חברי הצוות צריכים לבדוק ולהבין את הפרויקט המלא.

---

# Critical Path

```text
Dataset
   ↓
Data Model (3 indexes, release_year vs rating_year)
   ↓
Docker Stack
   ↓
Elasticsearch Mappings
   ↓
Spark ETL → Direct ES Write
   ↓
Gold Queries
   ↓
Ollama Query Generation + Validator
   ↓
Full Demo (local laptop)
```

**מודל הנתונים** הוא נקודת הסנכרון העיקרית. אין לתת למסלולי הפיתוח להתפצל לפני שמוסכמים שמות שדות, מטרות אינדקסים, והסמנטיקה של `release_year` לעומת `rating_year`.

---

# היקף פרויקט מומלץ

## לכלול
- MovieLens `ratings.csv`, `movies.csv`, `tags.csv`
- Docker Compose stack (Kafka, Spark, Elasticsearch, Kibana, Ollama, App)
- Kafka ל-ingestion של דירוגים (stream מדומה מ-CSV)
- Spark לטרנספורמציה, צבירה ו**כתיבה ישירה ל-Elasticsearch**
- שלושה אינדקסי Elasticsearch עם סמנטיקת שדות ברורה
- Kibana ל-dashboards אנליטיים
- Ollama (`llama3.2:3b`) לשפה טבעית → שאילתת Elasticsearch
- אימות שאילתות
- ממשק demo ב-Streamlit (לפטופ מקומי)
- ערכת הערכת AI (~20 שאלות)

## לא לכלול אלא אם יש זמן נוסף
- מנוע המלצות
- embeddings/vector search
- RAG
- מודלים של deep learning
- פריסה בענן
- `genome-scores.csv`, `genome-tags.csv`

המטרה היא לספק מערכת עובדת שהצוות כולו יכול להסביר בבהירות.
