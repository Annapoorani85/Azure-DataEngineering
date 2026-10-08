# Spark Declarative Pipelines (SDP): Medallion with Data Quality

# MAGIC **Use Case**: Build a production-grade medallion pipeline with built-in data quality expectations using Databricks SDP.

# MAGIC ## Key Concepts

# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **`@dp.table`** | Define a materialized view or streaming table |
# MAGIC | **`@dp.expect`** | Drop records that violate a quality rule (warn only) |
# MAGIC | **`@dp.expect_or_drop`** | Drop records that fail (enforce) |
# MAGIC | **`@dp.expect_or_fail`** | Fail the pipeline if any record violates |
# MAGIC | **Streaming tables** | Auto Loader-backed incremental ingestion |
# MAGIC | **Materialized views** | Batch-processed tables with automatic refresh |
# MAGIC | **Pipeline settings** | Configure scheduling, notifications, autoscaling |

# MAGIC ## Architecture
# MAGIC ```
# MAGIC   Cloud Files (CSV/JSON)
# MAGIC         │
# MAGIC         ▼
# MAGIC   ┌─────────────┐    @dp.expect_or_drop
# MAGIC   │   Bronze     │    (drop bad records)
# MAGIC   │  raw_events  │
# MAGIC   └──────┬──────┘
# MAGIC          │ @dp.expect
# MAGIC          ▼
# MAGIC   ┌─────────────┐    (warn on anomalies)
# MAGIC   │   Silver     │    Dedup + enrich
# MAGIC   │ cleaned_evt  │
# MAGIC   └──────┬──────┘
# MAGIC          │ @dp.expect_or_fail
# MAGIC          ▼
# MAGIC   ┌─────────────┐    (enforce business rules)
# MAGIC   │   Gold       │    Aggregations
# MAGIC   │ daily_metrics│
# MAGIC   └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Cell 1: Pipeline Configuration
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 1: Pipeline Configuration
# MAGIC
# MAGIC Define pipeline-level settings. These are typically set in the pipeline UI or via the API.
# MAGIC For reference, here is the JSON configuration:

# COMMAND ----------

# MAGIC %md
# MAGIC ```json
# MAGIC {
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "autoscale": {
# MAGIC         "min_workers": 1,
# MAGIC         "max_workers": 4,
# MAGIC         "mode": "ENHANCED"
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "development": true,
# MAGIC   "continuous": false,
# MAGIC   "channel": "CURRENT",
# MAGIC   "photon": true,
# MAGIC   "edition": "ADVANCED",
# MAGIC   "target": "demo.sdp",
# MAGIC   "configuration": {
# MAGIC     "pipelines.reset.allowed": false,
# MAGIC     "pipelines.streamVacuuming.enabled": true
# MAGIC   },
# MAGIC   "notifications": [
# MAGIC     {
# MAGIC       "email_recipients": ["data-team@company.com"],
# MAGIC       "alerts": ["on-update-failure", "on-flow-failure"]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 2: Bronze Layer — Streaming Table
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 2: Bronze Layer — Streaming Table with Auto Loader
# MAGIC
# MAGIC Ingest raw events from cloud storage with Auto Loader.  
# MAGIC Apply `@dp.expect_or_drop` to filter out malformed records.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit
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
@dp.table(
    name="bronze_events_raw",
    comment="Raw events ingested from cloud storage via Auto Loader",
    table_properties={"pipelines.reset.allowed": "false"},
    partition_cols=["event_type"],
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
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

# COMMAND ----------

# DBTITLE 1,Cell 3: Bronze DQ Summary View
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 3: Bronze Layer — Data Quality Dashboard View
# MAGIC
# MAGIC A view that summarises data quality metrics across all expectations.

# COMMAND ----------

@dp.temporary_view(
    name="bronze_dq_summary",
    comment="Data quality summary for bronze layer expectations",
)
def bronze_dq_summary():
    """
    Aggregates pass/fail counts per expectation rule.
    In production, these metrics flow to the pipeline event log automatically.
    """
    return (
        spark.read.table("bronze_events_raw")
        .groupBy("event_type")
        .agg(
            count("*").alias("total_records"),
            sum(when(col("event_value") > 0, 1).otherwise(0)).alias("positive_values"),
            sum(when(col("event_value") <= 0, 1).otherwise(0)).alias("non_positive_values"),
            countDistinct("user_id").alias("distinct_users"),
        )
        .orderBy("event_type")
    )

# COMMAND ----------

# DBTITLE 1,Cell 4: Silver Layer — Cleaned & Enriched
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 4: Silver Layer — Cleaned & Enriched Events
# MAGIC
# MAGIC Deduplicate, enrich with derived columns, and apply quality warnings.

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date, hour, dayofweek

@dp.table(
    name="silver_events_cleaned",
    comment="Cleaned and enriched events with deduplication and derived columns",
    table_properties={"pipelines.reset.allowed": "false"},
    partition_cols=["event_date"],
)
@dp.expect("positive_value", "event_value > 0")
@dp.expect("valid_ip_format", "source_ip RLIKE '^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$'")
@dp.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
def silver_events_cleaned():
    return (
        spark.readStream.table("bronze_events_raw")
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

# COMMAND ----------

# DBTITLE 1,Cell 5: Silver Layer — Reference Join
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 5: Silver Layer — Reference Data Join
# MAGIC
# MAGIC Enrich events with a static reference table (user dimension).

# COMMAND ----------

# Static reference table (batch-processed materialized view)
@dp.materialized_view(
    name="silver_user_profiles",
    comment="User profile reference data enriched into silver layer",
)
def silver_user_profiles():
    # In production, this would read from a dimension table
    return (
        spark.read.table("bronze_events_raw")
        .select("user_id")
        .distinct()
        .withColumn("profile_status", lit("active"))
        .withColumn("enriched_at", current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Cell 6: Gold Layer — Daily Metrics
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 6: Gold Layer — Daily Metrics Aggregation
# MAGIC
# MAGIC Business-ready aggregations with strict quality enforcement (`expect_or_fail`).

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, countDistinct, expr

@dp.table(
    name="gold_daily_event_metrics",
    comment="Daily event metrics per event type — business-ready aggregation",
    table_properties={
        "pipelines.reset.allowed": "false",
        "delta.enableChangeDataFeed": "true",
    },
)
@dp.expect_or_fail("non_null_event_date", "event_date IS NOT NULL")
@dp.expect_or_fail("non_null_event_type", "event_type IS NOT NULL")
def gold_daily_event_metrics():
    return (
        spark.readStream.table("silver_events_cleaned")
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

# COMMAND ----------

# DBTITLE 1,Cell 7: Gold Layer — User Activity
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 7: Gold Layer — User Activity Summary
# MAGIC
# MAGIC Per-user daily activity counts — useful for churn analysis and engagement scoring.

# COMMAND ----------

@dp.table(
    name="gold_user_activity",
    comment="Per-user daily activity summary for engagement analysis",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dp.expect_or_fail("valid_user_id", "user_id IS NOT NULL")
@dp.expect_or_fail("valid_date", "event_date IS NOT NULL")
def gold_user_activity():
    return (
        spark.readStream.table("silver_events_cleaned")
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

# COMMAND ----------

# DBTITLE 1,Cell 8: Gold Layer — Quality KPIs
# Databricks notebook source
# MAGIC %md
# MAGIC ## Cell 8: Gold Layer — Quality Metrics Report
# MAGIC
# MAGIC A view that tracks data quality KPIs across all layers.

# COMMAND ----------

@dp.temporary_view(
    name="gold_quality_kpis",
    comment="Data quality KPIs across pipeline layers",
)
def gold_quality_kpis():
    """Calculate quality KPIs: completeness, validity, uniqueness."""
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
                "round(silver_count / greatest(bronze_count, 1) * 100, 2)"
            ).alias("metric_value")
        )
        .unionByName(
            bronze_stats
            .crossJoin(silver_stats)
            .select(
                lit("silver_dedup_rate").alias("metric_name"),
                expr(
                    "round((bronze_count - silver_count) / greatest(bronze_count, 1) * 100, 2)"
                ).alias("metric_value")
            )
        )
    )

# COMMAND ----------

# DBTITLE 1,Key Takeaways
# MAGIC %md
# MAGIC # Key Takeaways
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **Streaming tables** | `readStream` + Auto Loader for incremental ingestion |
# MAGIC | **Materialized views** | `read` (batch) for dimension/reference tables |
# MAGIC | **`@dp.expect`** | Log warning, keep the row |
# MAGIC | **`@dp.expect_or_drop`** | Drop the row, log as metric |
# MAGIC | **`@dp.expect_or_fail`** | Fail the pipeline (strict enforcement) |
# MAGIC | **Auto Loader in SDP** | Idempotent file ingestion with schema evolution |
# MAGIC | **`pipelines.reset.allowed`** | Prevent full refresh (protect historical data) |
# MAGIC
# MAGIC ## Best Practices
# MAGIC 1. **Bronze → `expect_or_drop`** — tolerate bad source data, don't fail the pipeline
# MAGIC 2. **Silver → `expect`** — warn on anomalies for investigation, keep the data
# MAGIC 3. **Gold → `expect_or_fail`** — enforce business rules (downstream analytics depend on correctness)
# MAGIC 4. **Use `dropDuplicates`** for deduplication in streaming tables
# MAGIC 5. **Partition by date** — enables efficient incremental processing and pruning
# MAGIC 6. **Enable CDF on gold tables** — powers downstream CDC and incremental loads
# MAGIC 7. **Set `pipelines.reset.allowed = false`** — prevents accidental full refreshes in production
# MAGIC
# MAGIC > **Note**: This notebook is designed to be attached to a Lakeflow Declarative Pipeline.
# MAGIC > Create a pipeline in the Databricks UI, select this notebook as the source, and configure
# MAGIC > the target schema as `demo.sdp`.

# COMMAND ----------