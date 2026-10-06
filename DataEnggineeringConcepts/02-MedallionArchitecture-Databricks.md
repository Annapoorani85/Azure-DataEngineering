# Medallion Architecture in Databricks

## Overview

**Medallion Architecture** is a data design pattern used in modern data engineering to organize data into **multiple quality layers** as it moves from raw ingestion to business-ready analytics.

It was popularized by **Databricks** for building data lakes and lakehouses, but the concept can be applied across many data platforms.

The traditional Medallion Architecture consists of three main layers:

1. 🥉 **Bronze** — Raw Data
2. 🥈 **Silver** — Cleaned & Validated Data
3. 🥇 **Gold** — Business-Ready Data

---

# Medallion Architecture

```text
                    DATA SOURCES
                         │
                         ↓
┌──────────────────────────────────────────┐
│              🥉 BRONZE                  │
│──────────────────────────────────────────│
│ Raw Data                                 │
│ No / Minimal Transformations             │
│ Original Data                            │
│ Audit Copy                               │
│ Reprocessing / Replay                    │
└─────────────────────┬────────────────────┘
                      │
                      ↓
┌──────────────────────────────────────────┐
│              🥈 SILVER                  │
│──────────────────────────────────────────│
│ Cleaned Data                             │
│ Validated Data                           │
│ Joined Data                              │
│ Standardized Data                        │
│ Enriched Data                            │
└─────────────────────┬────────────────────┘
                      │
                      ↓
┌──────────────────────────────────────────┐
│               🥇 GOLD                   │
│──────────────────────────────────────────│
│ Aggregated Data                          │
│ Business KPIs                            │
│ Reporting Tables                         │
│ Analytics-Ready Data                     │
└─────────────────────┬────────────────────┘
                      │
             ┌────────┼─────────┐
             ↓        ↓         ↓
          Power BI  Tableau     ML
```

---

# 🥉 Bronze Layer — Raw Data

The **Bronze layer** stores data as close as possible to what was received from the source.

The primary goal is to preserve the original data and maintain an auditable copy.

### Example

Suppose an Orders source provides:

| OrderID | CustomerID | Amount | Currency |
| ------: | ---------: | -----: | -------- |
|     101 |          1 |   5000 | INR      |
|     102 |          2 |    120 | USD      |

The Bronze layer stores this data with minimal transformation.

### Characteristics

* Raw data
* Duplicate records may exist
* Missing values may exist
* Original source information is preserved
* Useful for auditing
* Useful for reprocessing
* Useful for replaying transformations
* Provides a historical record of what was received

Think of Bronze as:

> **"The source-of-record copy of what entered the data platform."**

---

# 🥈 Silver Layer — Cleaned & Validated Data

The **Silver layer** transforms raw data into a more reliable and standardized format.

This is where many of the core data engineering transformations take place.

### Typical Operations

* Remove duplicates
* Handle missing values
* Fix or reject invalid dates
* Standardize column names
* Convert data types
* Standardize formats
* Apply data quality rules
* Join related datasets
* Enrich data
* Apply business rules

### Example

Bronze:

| OrderID | CustomerID | Amount | Currency |
| ------: | ---------: | -----: | -------- |
|     101 |          1 |   5000 | INR      |
|     101 |          1 |   5000 | INR      |
|     102 |          2 |   NULL | USD      |

After Silver transformations:

| OrderID | CustomerID |          Amount | Currency |
| ------: | ---------: | --------------: | -------- |
|     101 |          1 |            5000 | INR      |
|     102 |          2 | Validated value | USD      |

The duplicate has been removed and the missing value has been handled according to the applicable business rule.

### Purpose

Silver should provide data that is **reliable enough for downstream analytical processing**.

---

# 🥇 Gold Layer — Business-Ready Data

The **Gold layer** contains data that has been prepared for specific business and analytical use cases.

Examples include:

* Daily sales
* Monthly revenue
* Customer lifetime value
* Product performance
* Regional sales
* Customer segmentation
* Executive dashboards
* Business KPIs

Instead of storing every individual order, Gold may contain summarized information.

### Example

| Date       | Revenue |
| ---------- | ------: |
| 2026-07-01 | 250,000 |
| 2026-07-02 | 275,000 |

This is the type of data that reporting and BI tools can query directly.

---

# Why Not Skip Bronze?

A common question is:

> **"Why don't we just clean the data immediately and store only Silver?"**

Consider this scenario:

Yesterday's source data contained mistakes.

If you only keep the cleaned version, you may no longer be able to:

* Verify what originally arrived
* Investigate ingestion problems
* Reprocess the data using updated business rules
* Recreate downstream datasets
* Compare source data with transformed data
* Audit historical ingestion

By keeping Bronze, you preserve the original data.

This gives you the ability to rebuild downstream layers when necessary.

```text
                    Bronze
                      │
             ┌────────┴────────┐
             ↓                 ↓
          Silver             Replay
             │                 │
             ↓                 │
           Gold ←──────────────┘
```

For example, if a business rule changes, you can potentially reprocess Bronze data with the new rule instead of requesting the source system to resend historical data.

---

# Typical Technologies

| Layer         | Common Technologies                                                     |
| ------------- | ----------------------------------------------------------------------- |
| **Bronze**    | Delta Lake, Parquet, Apache Iceberg, Amazon S3, Azure Data Lake Storage |
| **Silver**    | Apache Spark, SQL, dbt, Databricks                                      |
| **Gold**      | Spark SQL, SQL, dbt, Databricks                                         |
| **Reporting** | Power BI, Tableau, Looker                                               |

In many architectures:

> **Data engineers primarily maintain Bronze and Silver, while business users and analysts primarily consume Gold.**

The exact ownership varies by organization.

---

# Medallion Architecture vs ETL / ELT

These concepts are related, but they describe **different things**.

| ETL / ELT                                   | Medallion Architecture                                  |
| ------------------------------------------- | ------------------------------------------------------- |
| Describes **how** data moves and transforms | Describes **how** data is organized into quality layers |
| Focuses on the pipeline process             | Focuses on the structure and refinement of data         |
| ETL = Extract → Transform → Load            | Bronze → Silver → Gold                                  |
| ELT = Extract → Load → Transform            | Multiple refinement stages                              |
| Describes data processing steps             | Describes data organization                             |
| Can use ETL or ELT                          | Often implemented with ELT in modern lakehouses         |

### Simple way to remember

**ETL / ELT = Process**

**Medallion = Architecture**

---

# ETL / ELT and Medallion Work Together

These concepts are not competing architectures.

They complement each other.

### ETL / ELT describes:

> **How we move and transform the data.**

### Medallion Architecture describes:

> **Where the data exists at different stages of refinement.**

A modern data pipeline might look like:

```text
Sources
   │
   ↓
Extract & Load
   │
   ↓
🥉 Bronze
Raw Data
   │
   │ Transform
   ↓
🥈 Silver
Clean / Validated / Integrated Data
   │
   │ Transform
   ↓
🥇 Gold
Business / Analytics Data
   │
   ├───────────┬────────────┐
   ↓           ↓            ↓
Power BI    Tableau         ML
```

---

# Real-World Architecture: More Than Three Layers

The **Bronze → Silver → Gold** model is a useful conceptual framework, but real-world data platforms do not always have exactly three physical layers.

A company may have **many stages of refinement**.

For example:

```text
Source
  │
  ↓
Product 0
Raw Data
  │
  ↓
Product 1
Basic Cleaning
  │
  ↓
Product 2
Standardization
  │
  ↓
Product 3
Integration
  │
  ↓
Product 4
Business Rules
  │
  ↓
Product 5
Aggregations
  │
  ↓
Product 6
Business KPIs
  │
  ↓
Reporting / Analytics
```

Each stage can add additional value.

For example:

```text
Product 0
   ↓
Raw source data
   ↓
Product 1
   + Cleaning
   ↓
Product 2
   + Standardization
   ↓
Product 3
   + Data integration
   ↓
Product 4
   + Enrichment
   ↓
Product 5
   + Business measures
   ↓
Product 6
   + KPIs / Aggregations
```

The important idea is:

> **At every stage, data becomes progressively more refined, enriched, and useful.**

---

# Progressive Data Enrichment

Consider an e-commerce example.

### Product 0 — Raw Orders

```text
OrderID
CustomerID
ProductID
Quantity
Amount
OrderDate
```

### Product 1 — Cleaned Orders

Add:

* Duplicate removal
* Validated dates
* Validated quantities
* Standardized data types

### Product 2 — Enriched Orders

Join with:

```text
Customer
Product
Store
Region
```

Now we might have:

```text
OrderID
CustomerName
ProductName
Category
Region
Quantity
Amount
OrderDate
```

### Product 3 — Business Measures

Calculate:

```text
Revenue
Discount
Net Sales
Profit
Profit Margin
```

### Product 4 — Business KPIs

Aggregate the data:

```text
Daily Revenue
Monthly Revenue
Average Order Value
Customer Lifetime Value
Profit Margin
Regional Sales
Product Performance
```

The data has become progressively more useful:

```text
Raw
 ↓
Clean
 ↓
Standardized
 ↓
Integrated
 ↓
Enriched
 ↓
Business Measures
 ↓
KPIs
```

---

# The Important Mental Model

Don't think of Medallion Architecture as:

> **"There must always be exactly three tables: Bronze, Silver, and Gold."**

Instead, think of it as:

> **"Data moves through progressively refined stages, with each stage adding quality, context, business meaning, or analytical value."**

A simple conceptual representation is:

```text
                 DATA QUALITY / BUSINESS VALUE
                              ↑
                              │
                              │
       Raw ───────→ Clean ───────→ Enriched ───────→ Business
        │             │              │                 │
        │             │              │                 │
      Bronze        Silver        Intermediate        Gold
                              │
                              │
                              ↓
                        More refinement
```

The number of layers or datasets between raw and final consumption can vary based on:

* Business requirements
* Data complexity
* Number of source systems
* Transformation complexity
* Governance requirements
* Data quality requirements
* Performance requirements
* Reusability requirements

---

# Key Takeaways

| Concept                    | Meaning                                                        |
| -------------------------- | -------------------------------------------------------------- |
| 🥉 **Bronze**              | Raw, source-aligned data                                       |
| 🥈 **Silver**              | Cleaned, validated, standardized and integrated data           |
| 🥇 **Gold**                | Business-ready, analytical and reporting data                  |
| **ETL**                    | Extract → Transform → Load                                     |
| **ELT**                    | Extract → Load → Transform                                     |
| **Medallion Architecture** | Organizes data into progressively refined layers               |
| **Enrichment**             | Adds useful context from other data sources                    |
| **Aggregation**            | Converts detailed data into summaries                          |
| **KPI**                    | Business measure used to track performance                     |
| **Data Product**           | A curated dataset designed for a specific consumer or use case |

---

# Final Mental Model

The entire concept can be summarized as:

```text
                         DATA SOURCES
                              │
                              ↓
                     ┌─────────────────┐
                     │   🥉 BRONZE     │
                     │    Raw Data     │
                     └────────┬────────┘
                              │
                         Clean / Validate
                              ↓
                     ┌─────────────────┐
                     │   🥈 SILVER     │
                     │ Clean + Trusted │
                     └────────┬────────┘
                              │
                      Join / Enrich /
                     Transform / Measure
                              ↓
                     ┌─────────────────┐
                     │    🥇 GOLD      │
                     │ Business Ready  │
                     │ KPIs / Reports  │
                     └────────┬────────┘
                              │
                  ┌───────────┼───────────┐
                  ↓           ↓           ↓
               Power BI    Tableau       ML
```

### The core idea

**Bronze → Preserve**

**Silver → Refine**

**Gold → Serve**

And in real-world data platforms:

> **You can have multiple intermediate data products or refinement stages. Each stage progressively improves the quality, context, usability, and business value of the data.**
