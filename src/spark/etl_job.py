"""Stage 6 Spark ETL — Kafka ratings + static movies/tags → Elasticsearch."""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    avg,
    coalesce,
    col,
    collect_set,
    concat,
    count,
    from_json,
    from_unixtime,
    length,
    lit,
    lower,
    regexp_extract,
    round,
    size,
    split,
    sum,
    to_timestamp,
    trim,
    when,
    year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import (
    DATA_RAW_PATH,
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PORT,
    INDEX_MOVIE_RATINGS_BY_RATING_YEAR,
    INDEX_MOVIES,
    INDEX_MOVIES_BY_RELEASE_YEAR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_RATINGS,
)

KAFKA_MESSAGE_SCHEMA = StructType(
    [
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def create_spark() -> SparkSession:
    return SparkSession.builder.appName("movielens-etl").getOrCreate()


def read_ratings_from_kafka(spark: SparkSession) -> DataFrame:
    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW_RATINGS)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(
        from_json(col("value").cast("string"), KAFKA_MESSAGE_SCHEMA).alias("data")
    ).select("data.*")

    return (
        parsed.withColumnRenamed("movieId", "movie_id")
        .withColumnRenamed("userId", "user_id")
        .withColumn(
            "timestamp_str",
            col("timestamp").cast("string"),
        )
        .withColumn(
            "rating_year",
            when(
                col("timestamp_str").rlike(r"^\d+$"),
                year(from_unixtime(col("timestamp_str").cast(LongType()))),
            ).otherwise(year(to_timestamp(col("timestamp_str")))),
        )
        .drop("timestamp_str")
        .filter((col("rating") >= 0.5) & (col("rating") <= 5.0))
        .filter((col("rating") * 2).cast("int") == (col("rating") * 2))
        .filter(col("movie_id").isNotNull())
    )


def read_movies(spark: SparkSession) -> DataFrame:
    movies = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{DATA_RAW_PATH}/movies.csv")
        .withColumnRenamed("movieId", "movie_id")
    )
    release_year_raw = regexp_extract(col("title"), r"\((\d{4})\)$", 1)
    return movies.withColumn(
        "release_year",
        when(length(release_year_raw) > 0, release_year_raw.cast(IntegerType())),
    ).withColumn(
        "genres",
        when(col("genres") == "(no genres listed)", array().cast("array<string>")).otherwise(
            split(col("genres"), "\\|")
        ),
    )


def read_tags_by_movie(spark: SparkSession) -> DataFrame:
    tags = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{DATA_RAW_PATH}/tags.csv")
        .withColumnRenamed("movieId", "movie_id")
        .withColumn("tag", lower(trim(col("tag"))))
        .filter(length(col("tag")) > 0)
    )
    return tags.groupBy("movie_id").agg(
        collect_set("tag").alias("tags"),
    ).withColumn("tag_count", size(col("tags")))


def build_movies_index(ratings: DataFrame, movies: DataFrame, tags_by_movie: DataFrame) -> DataFrame:
    rating_stats = ratings.groupBy("movie_id").agg(
        round(avg("rating"), 4).alias("average_rating"),
        count("*").alias("rating_count"),
    )
    rated_movie_ids = ratings.select("movie_id").distinct()
    movies_in_sample = movies.join(rated_movie_ids, "movie_id", "inner")

    movies_out = (
        movies_in_sample.join(rating_stats, "movie_id", "inner")
        .join(tags_by_movie, "movie_id", "left")
        .withColumn("tags", coalesce(col("tags"), array().cast("array<string>")))
        .withColumn("tag_count", coalesce(col("tag_count"), lit(0)))
        .select(
            "movie_id",
            "title",
            "release_year",
            "genres",
            "average_rating",
            "rating_count",
            "tags",
            "tag_count",
        )
    )
    return movies_out


def build_release_year_index(movies_out: DataFrame) -> DataFrame:
    weighted = movies_out.filter(col("release_year").isNotNull()).withColumn(
        "weighted_sum",
        col("average_rating") * col("rating_count"),
    )
    return (
        weighted.groupBy("release_year")
        .agg(
            count("movie_id").alias("movie_count"),
            sum("rating_count").alias("total_rating_count"),
            round(sum("weighted_sum") / sum("rating_count"), 4).alias("average_rating"),
        )
        .select("release_year", "movie_count", "total_rating_count", "average_rating")
    )


def build_rating_year_index(ratings: DataFrame, movies: DataFrame) -> DataFrame:
    rating_year_stats = ratings.groupBy("movie_id", "rating_year").agg(
        round(avg("rating"), 4).alias("average_rating"),
        count("*").alias("rating_count"),
    )
    titles = movies.select("movie_id", "title")
    return (
        rating_year_stats.join(titles, "movie_id", "left")
        .withColumn(
            "movie_rating_year_id",
            concat(col("movie_id").cast("string"), lit("_"), col("rating_year").cast("string")),
        )
        .select(
            "movie_id",
            "title",
            "rating_year",
            "rating_count",
            "average_rating",
            "movie_rating_year_id",
        )
    )


def write_to_elasticsearch(df: DataFrame, index_name: str, id_field: str) -> None:
    (
        df.write.format("org.elasticsearch.spark.sql")
        .option("es.nodes", ELASTICSEARCH_HOST)
        .option("es.port", ELASTICSEARCH_PORT)
        .option("es.nodes.wan.only", "true")
        .option("es.resource", index_name)
        .option("es.mapping.id", id_field)
        .option("es.write.operation", "upsert")
        .mode("append")
        .save()
    )


def run_etl(spark: SparkSession) -> dict[str, int]:
    print(f"Reading ratings from Kafka topic `{KAFKA_TOPIC_RAW_RATINGS}`...")
    ratings = read_ratings_from_kafka(spark)
    ratings_count = ratings.count()
    print(f"  valid ratings: {ratings_count:,}")

    if ratings_count == 0:
        raise RuntimeError(
            "No ratings in Kafka — run scripts/run_producer.py first "
            f"(topic `{KAFKA_TOPIC_RAW_RATINGS}`)"
        )

    print("Reading static movies and tags...")
    movies = read_movies(spark)
    tags_by_movie = read_tags_by_movie(spark)

    movies_out = build_movies_index(ratings, movies, tags_by_movie)
    release_year_out = build_release_year_index(movies_out)
    rating_year_out = build_rating_year_index(ratings, movies)

    movies_count = movies_out.count()
    release_year_count = release_year_out.count()
    rating_year_count = rating_year_out.count()

    print(
        f"Built outputs: movies={movies_count:,}, "
        f"release_year={release_year_count:,}, rating_year={rating_year_count:,}"
    )

    print(f"Writing to Elasticsearch at {ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}...")
    write_to_elasticsearch(movies_out, INDEX_MOVIES, "movie_id")
    print(f"  wrote index `{INDEX_MOVIES}`")

    write_to_elasticsearch(release_year_out, INDEX_MOVIES_BY_RELEASE_YEAR, "release_year")
    print(f"  wrote index `{INDEX_MOVIES_BY_RELEASE_YEAR}`")

    write_to_elasticsearch(rating_year_out, INDEX_MOVIE_RATINGS_BY_RATING_YEAR, "movie_rating_year_id")
    print(f"  wrote index `{INDEX_MOVIE_RATINGS_BY_RATING_YEAR}`")

    return {
        "ratings": ratings_count,
        "movies": movies_count,
        "release_year": release_year_count,
        "rating_year": rating_year_count,
    }


def main() -> int:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = run_etl(spark)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()

    print(
        "\nSpark ETL complete:\n"
        f"  ratings processed: {stats['ratings']:,}\n"
        f"  movies index:      {stats['movies']:,}\n"
        f"  release_year:      {stats['release_year']:,}\n"
        f"  rating_year:       {stats['rating_year']:,}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
