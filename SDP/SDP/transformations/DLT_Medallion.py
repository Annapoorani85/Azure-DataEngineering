# DLT (SDP): Medallion with Data Quality

import dlt
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# Define schema for source files
EVENT_SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_value", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("source_ip", StringType(), True),
])

# Bronze: streaming table with Auto Loader
@dlt.table(
    name="bronze_events_raw",
    comment="Raw events ingested from cloud storage via Auto Loader",
    table_properties={"pipelines.reset.allowed": "false"},
    partition_cols=["event_type"],
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def bronze_events_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/demo/sdp/_schemas/bronze")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .schema(EVENT_SCHEMA)
        .load("/Volumes/demo/sdp/events/")
        .withColumn("source_file", input_file_name())
        .withColumn("ingest_time", current_timestamp())
    )


@dlt.view(
    name="bronze_dq_summary",
    comment="Data quality summary for bronze layer expectations",
)
def bronze_dq_summary():
    """
    Aggregates pass/fail counts per expectation rule.
    In production, these metrics flow to the pipeline event log automatically.
    """
    return (
        dlt.read("bronze_events_raw")
        .groupBy("event_type")
        .agg(
            spark.functions.count("*").alias("total_records"),
            spark.functions.sum(spark.functions.when(col("event_value") > 0, 1).otherwise(0)).alias("positive_values"),
            spark.functions.sum(spark.functions.when(col("event_value") <= 0, 1).otherwise(0)).alias("non_positive_values"),
            spark.functions.countDistinct("user_id").alias("distinct_users"),
        )
        .orderBy("event_type")
    )


from pyspark.sql.functions import col, when, to_date, hour, dayofweek

@dlt.table(
    name="silver_events_cleaned",
    comment="Cleaned and enriched events with deduplication and derived columns",
    table_properties={"pipelines.reset.allowed": "false"},
    partition_cols=["event_date"],
)
@dlt.expect("positive_value", "event_value > 0")
@dlt.expect("valid_ip_format", "source_ip RLIKE '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
def silver_events_cleaned():
    return (
        dlt.read_stream("bronze_events_raw")
        # Deduplicate by event_id (keep latest)
        .dropDuplicates(["event_id"])
        # Enrich with derived columns
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("day_of_week", dayofweek(col("event_timestamp")))
        .withColumn("is_weekend", when(dayofweek(col("event_timestamp")).isin([1, 7]), True).otherwise(False))
        .withColumn("value_category", 
            when(col("event_value") < 50, "low")
            .when(col("event_value") < 200, "medium")
            .otherwise("high")
        )
    )


# Static reference table (batch-processed materialized view)
@dlt.table(
    name="silver_user_profiles",
    comment="User profile reference data enriched into silver layer",
)
def silver_user_profiles():
    # In production, this would read from a dimension table
    return (
        dlt.read("bronze_events_raw")
        .select("user_id")
        .distinct()
        .withColumn("profile_status", lit("active"))
        .withColumn("enriched_at", current_timestamp())
    )


from pyspark.sql.functions import col, count, sum, avg, countDistinct, expr

@dlt.table(
    name="gold_daily_event_metrics",
    comment="Daily event metrics per event type — business-ready aggregation",
    table_properties={
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_or_fail("non_null_event_date", "event_date IS NOT NULL")
@dlt.expect_or_fail("non_null_event_type", "event_type IS NOT NULL")
def gold_daily_event_metrics():
    return (
        dlt.read_stream("silver_events_cleaned")
        .groupBy("event_date", "event_type", "is_weekend")
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            sum("event_value").alias("total_value"),
            avg("event_value").alias("avg_value"),
            expr("percentile_approx(event_value, 0.95)").alias("p95_value"),
        )
        .withColumn("updated_at", current_timestamp())
    )


@dlt.table(
    name="gold_user_activity",
    comment="Per-user daily activity summary for engagement analysis",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dlt.expect_or_fail("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_fail("valid_date", "event_date IS NOT NULL")
def gold_user_activity():
    return (
        dlt.read_stream("silver_events_cleaned")
        .groupBy("event_date", "user_id")
        .agg(
            count("*").alias("daily_event_count"),
            countDistinct("event_type").alias("event_type_variety"),
            sum("event_value").alias("daily_total_value"),
            max("event_timestamp").alias("last_activity"),
        )
        .withColumn("activity_tier",
            when(col("daily_event_count") >= 10, "power_user")
            .when(col("daily_event_count") >= 3, "regular_user")
            .otherwise("casual_user")
        )
    )


@dlt.view(
    name="gold_quality_kpis",
    comment="Data quality KPIs across pipeline layers",
)
def gold_quality_kpis():
    """Calculate quality KPIs: completeness, validity, uniqueness."""
    bronze = dlt.read("bronze_events_raw")
    silver = dlt.read("silver_events_cleaned")
    
    bronze_count = bronze.count()
    silver_count = silver.count()
    
    return spark.createDataFrame([
        ("bronze_to_silver_retention", round(silver_count / max(bronze_count, 1) * 100, 2)),
        ("silver_dedup_rate", round((bronze_count - silver_count) / max(bronze_count, 1) * 100, 2)),
    ], ["metric_name", "metric_value"])