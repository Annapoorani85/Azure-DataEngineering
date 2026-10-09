# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ai_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Databricks SDP Medallion Pipeline Setup
# MAGIC
# MAGIC Bronze → Silver → Gold pipeline using Auto Loader and Lakeflow Declarative Pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define schema for source files

# COMMAND ----------

# Importing the Databricks pipeline API
from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    current_timestamp,
    input_file_name
)

# Importing Spark's built-in data types.
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType
)

# StructType represents the entire schema of a DataFrame
# Here we are defining the structure of my incoming event data like we do in CREATE TABLE command 
# A StructField represents one column. Syntax : StructField("column_name",DataType(),nullable)  

EVENT_SCHEMA = StructType([
    StructField("event_id", LongType(), True),
    StructField("user_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_value", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("source_ip", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: streaming table with Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC @dp is a decorator.
# MAGIC - This decorator is to give instruction about the function below it - bronze_events_raw().
# MAGIC - The decorator, tells Databricks that DataFrame returned by this function should become a table in my pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC In the @dp.table decorator we are providing 
# MAGIC - Table name 
# MAGIC - Description about the table
# MAGIC - Pipeline property : reset = false : Don't allow this dataset to be reset/full-refreshed through the reset mechanism.
# MAGIC - Partition is based on : event_type( login, purchase, page_view, logout, search)

# COMMAND ----------

# MAGIC %md
# MAGIC @dp.expect_or_drop
# MAGIC - This is a data quality expectation.
# MAGIC - Our expectation is every record should have an event_id.
# MAGIC - event_id IS NOT NULL -> FALSE -> DROP RECORD
# MAGIC - Same for the user_id

# COMMAND ----------

# MAGIC %md
# MAGIC spark.readStream 
# MAGIC - It is to read the data as a stream.
# MAGIC - In case fo batch use : spark.read
# MAGIC - It means Watch/process incoming data -> Process new data incrementally

# COMMAND ----------

@dp.table(
    name="bronze_events_raw",
    comment="Raw events ingested from cloud storage via Auto Loader",
    table_properties={
        "pipelines.reset.allowed": "false"
    },
    partition_cols=["event_type"]
)
# This function produces the Bronze DataFrame
# here we are just returing the data as a dataframe, this function doesnt write a table
def bronze_events_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/demo/sdp/pipeline_metadata/bronze/"
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
        .schema(EVENT_SCHEMA)
        .load("/Volumes/demo/sdp/events/")
        .withColumn("source_file", input_file_name())
        .withColumn("ingest_time", current_timestamp())
    )

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

@dp.temporary_view(
    name="bronze_dq_summary",
    comment="Data quality summary for bronze layer expectations"
)
def bronze_dq_summary():

    return (
        spark.read.table("bronze_events_raw")
        .groupBy("event_type")
        .agg(
            count("*").alias("total_records"),
            sum(
                when(col("event_value") > 0, 1).otherwise(0)
            ).alias("positive_values"),
            sum(
                when(col("event_value") <= 0, 1).otherwise(0)
            ).alias("non_positive_values"),
            countDistinct("user_id").alias("distinct_users")
        )
        .orderBy("event_type")
    )

# COMMAND ----------

@dp.table(
    name="silver_events_cleaned",
    comment="Cleaned and enriched events with deduplication and derived columns",
    table_properties={
        "pipelines.reset.allowed": "false"
    },
    partition_cols=["event_date"]
)
@dp.expect(
    "positive_value",
    "event_value > 0"
)
@dp.expect(
    "valid_ip_format",
    "source_ip RLIKE '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'"
)
@dp.expect_or_drop(
    "valid_timestamp",
    "event_timestamp IS NOT NULL"
)
def silver_events_cleaned():

    return (
        spark.readStream.table("bronze_events_raw")
        .dropDuplicates(["event_id"])
        .withColumn("event_date", to_date(col("event_timestamp")))
        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("day_of_week", dayofweek(col("event_timestamp")))
        .withColumn(
            "is_weekend",
            when(
                dayofweek(col("event_timestamp")).isin([1, 7]),
                True
            ).otherwise(False)
        )
        .withColumn(
            "value_category",
            when(col("event_value") < 50, "low")
            .when(col("event_value") < 200, "medium")
            .otherwise("high")
        )
    )

# COMMAND ----------

@dp.materialized_view(
    name="silver_user_profiles",
    comment="User profile reference data enriched into silver layer"
)
def silver_user_profiles():

    return (
        spark.read.table("bronze_events_raw")
        .select("user_id")
        .distinct()
        .withColumn("profile_status", lit("active"))
        .withColumn("enriched_at", current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="gold_daily_event_metrics",
    comment="Daily event metrics per event type — business-ready aggregation",
    table_properties={
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true"
    }
)
@dp.expect_or_fail(
    "non_null_event_date",
    "event_date IS NOT NULL"
)
@dp.expect_or_fail(
    "non_null_event_type",
    "event_type IS NOT NULL"
)
def gold_daily_event_metrics():

    return (
        spark.readStream.table("silver_events_cleaned")
        .groupBy(
            "event_date",
            "event_type",
            "is_weekend"
        )
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            sum("event_value").alias("total_value"),
            avg("event_value").alias("avg_value"),
            expr(
                "percentile_approx(event_value, 0.95)"
            ).alias("p95_value")
        )
        .withColumn("updated_at", current_timestamp())
    )

# COMMAND ----------

@dp.table(
    name="gold_user_activity",
    comment="Per-user daily activity summary for engagement analysis",
    table_properties={
        "pipelines.reset.allowed": "false"
    }
)
@dp.expect_or_fail(
    "valid_user_id",
    "user_id IS NOT NULL"
)
@dp.expect_or_fail(
    "valid_date",
    "event_date IS NOT NULL"
)
def gold_user_activity():

    return (
        spark.readStream.table("silver_events_cleaned")
        .groupBy("event_date", "user_id")
        .agg(
            count("*").alias("daily_event_count"),
            countDistinct("event_type").alias("event_type_variety"),
            sum("event_value").alias("daily_total_value"),
            max("event_timestamp").alias("last_activity")
        )
        .withColumn(
            "activity_tier",
            when(col("daily_event_count") >= 10, "power_user")
            .when(col("daily_event_count") >= 3, "regular_user")
            .otherwise("casual_user")
        )
    )

# COMMAND ----------

@dp.temporary_view(
    name="gold_quality_kpis",
    comment="Data quality KPIs across pipeline layers"
)
def gold_quality_kpis():

    bronze = spark.read.table("bronze_events_raw")
    silver = spark.read.table("silver_events_cleaned")

    bronze_stats = bronze.agg(
        count("*").alias("bronze_count")
    )

    silver_stats = silver.agg(
        count("*").alias("silver_count")
    )

    return (
        bronze_stats
        .crossJoin(silver_stats)
        .select(
            lit("bronze_to_silver_retention").alias("metric_name"),
            expr(
                """
                round(
                    silver_count /
                    greatest(bronze_count, 1) * 100,
                    2
                )
                """
            ).alias("metric_value")
        )
        .unionByName(
            bronze_stats
            .crossJoin(silver_stats)
            .select(
                lit("silver_dedup_rate").alias("metric_name"),
                expr(
                    """
                    round(
                        (bronze_count - silver_count) /
                        greatest(bronze_count, 1) * 100,
                        2
                    )
                    """
                ).alias("metric_value")
            )
        )
    )