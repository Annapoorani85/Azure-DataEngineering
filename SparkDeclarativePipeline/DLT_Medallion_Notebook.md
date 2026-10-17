%md
# Spark Declarative Pipelines (SDP): Medallion with Data Quality

**Use Case**: Build a production-grade medallion pipeline with built-in data quality expectations using Databricks SDP.

## Key Concepts

| Feature | Description |
|---------|-------------|
| **`@dlt.table`** | Define a materialized view or streaming table |
| **`@dlt.expect`** | Drop records that violate a quality rule (warn only) |
| **`@dlt.expect_or_drop`** | Drop records that fail (enforce) |
| **`@dlt.expect_or_fail`** | Fail the pipeline if any record violates |
| **Streaming tables** | Auto Loader-backed incremental ingestion |
| **Materialized views** | Batch-processed tables with automatic refresh |
| **Pipeline settings** | Configure scheduling, notifications, autoscaling |

## Architecture

```
  Cloud Files (CSV/JSON)
        │
        ▼
  ┌─────────────┐    @dlt.expect_or_drop
  │   Bronze     │    (drop bad records)
  │  raw_events  │
  └──────┬──────┘
         │ @dlt.expect
         ▼
  ┌─────────────┐    (warn on anomalies)
  │   Silver     │    Dedup + enrich
  │ cleaned_evt  │
  └──────┬──────┘
         │ @dlt.expect_or_fail
         ▼
  ┌─────────────┐    (enforce business rules)
  │   Gold       │    Aggregations
  │ daily_metrics│
  └─────────────┘
```

---
%md
## Cell 1: Pipeline Configuration

Define pipeline-level settings. These are typically set in the pipeline UI or via the API.
For reference, here is the JSON configuration:
%md
```json
{
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 1,
        "max_workers": 4,
        "mode": "ENHANCED"
      }
    }
  ],
  "development": true,
  "continuous": false,
  "channel": "CURRENT",
  "photon": true,
  "edition": "ADVANCED",
  "target": "demo.sdp",
  "configuration": {
    "pipelines.reset.allowed": false,
    "pipelines.streamVacuuming.enabled": true
  },
  "notifications": [
    {
      "email_recipients": ["data-team@company.com"],
      "alerts": ["on-update-failure", "on-flow-failure"]
    }
  ]
}
```
%md
## Cell 2: Bronze Layer — Streaming Table with Auto Loader

Ingest raw events from cloud storage with Auto Loader.  
Apply `@dlt.expect_or_drop` to filter out malformed records.
import dlt
from pyspark.sql.functions import current_timestamp, input_file_name
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
%md
| Code                  | Meaning                                      |
| --------------------- | -------------------------------------------- |
| `@dlt.table`          | Create a pipeline-managed table              |
| `@dlt.expect_or_drop` | Drop records violating a quality rule        |
| `spark.readStream`    | Read continuously/incrementally              |
| `cloudFiles`          | Use Auto Loader                              |
| `EVENT_SCHEMA`        | Explicit expected structure of incoming data |
| `schemaLocation`      | Store Auto Loader schema metadata            |
| `schemaEvolutionMode` | Control how new columns are handled          |
| `load(...)`           | Location of incoming files                   |
| `input_file_name()`   | Add source file information                  |
| `current_timestamp()` | Add ingestion/processing time                |

%md
## Cell 3: Bronze Layer — Data Quality Dashboard View

A view that summarises data quality metrics across all expectations.
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
%md
## Cell 4: Silver Layer — Cleaned & Enriched Events

Deduplicate, enrich with derived columns, and apply quality warnings.
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
%md
## Cell 5: Silver Layer — Reference Data Join

Enrich events with a static reference table (user dimension).
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
%md
## Cell 6: Gold Layer — Daily Metrics Aggregation

Business-ready aggregations with strict quality enforcement (`expect_or_fail`).
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
%md
## Cell 7: Gold Layer — User Activity Summary

Per-user daily activity counts — useful for churn analysis and engagement scoring.
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
%md
## Cell 8: Gold Layer — Quality Metrics Report

A view that tracks data quality KPIs across all layers.
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
%md
# Key Takeaways

| Concept | Description |
|---------|-------------|
| **Streaming tables** | `readStream` + Auto Loader for incremental ingestion |
| **Materialized views** | `read` (batch) for dimension/reference tables |
| **`@dlt.expect`** | Log warning, keep the row |
| **`@dlt.expect_or_drop`** | Drop the row, log as metric |
| **`@dlt.expect_or_fail`** | Fail the pipeline (strict enforcement) |
| **Auto Loader in SDP** | Idempotent file ingestion with schema evolution |
| **`pipelines.reset.allowed`** | Prevent full refresh (protect historical data) |

## Best Practices
1. **Bronze → `expect_or_drop`** — tolerate bad source data, don't fail the pipeline
2. **Silver → `expect`** — warn on anomalies for investigation, keep the data
3. **Gold → `expect_or_fail`** — enforce business rules (downstream analytics depend on correctness)
4. **Use `dropDuplicates`** for deduplication in streaming tables
5. **Partition by date** — enables efficient incremental processing and pruning
6. **Enable CDF on gold tables** — powers downstream CDC and incremental loads
7. **Set `pipelines.reset.allowed = false`** — prevents accidental full refreshes in production

> **Note**: This notebook is designed to be attached to a Spark Declarative Pipeline.
> Create a pipeline in the Databricks UI, select this notebook as the source, and configure
> the target schema as `demo.sdp`.
