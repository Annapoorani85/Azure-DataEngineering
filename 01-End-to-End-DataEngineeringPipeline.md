# End-to-End Data Engineering Pipeline

This is one of the **core responsibilities of a Data Engineer**.

The goal is to transform **raw, messy data** into **analytics-ready data** that business users, analysts, and data scientists can trust.

---

## End-to-End Data Engineering Pipeline

```text
                         Data Sources
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
      Oracle DB            REST API           CSV Files
          │                   │                   │
          └───────────────────┼───────────────────┘
                              │
                       1. Data Ingestion
                              │
                       Raw / Landing Zone
                              │
                       2. ETL / ELT Process
                              │
                       Data Cleaning
                              │
                       Data Transformation
                              │
                       Data Integration
                              │
                    Data Validation & Quality
                              │
                    Aggregation / Enrichment
                              │
                    Analytics-Ready Data (Gold)
                              │
                ┌─────────────┼─────────────┐
                │             │             │
             Power BI      Tableau          ML
                              │
                           Reports
```

---

# Step 1: Data Ingestion

The first step is **collecting data from different systems**.

### Example Sources

| Source    | Example Data     |
| --------- | ---------------- |
| Oracle    | Customer data    |
| SAP       | Product data     |
| REST API  | Orders           |
| CSV files | Sales            |
| Kafka     | Real-time events |

### Example

**Oracle**

| CustomerID | Name  |
| ---------: | ----- |
|        101 | Alice |

**REST API**

```json
{
  "OrderID": 1001,
  "CustomerID": 101
}
```

At this stage, **nothing is modified yet**.

The goal is simply to collect the data.

---

# Step 2: Raw / Landing / Bronze Layer

The data is stored **exactly as received**.

* No changes
* No cleaning
* No transformation

### Purpose

The raw layer is useful for:

* Backup
* Audit
* Reprocessing
* Troubleshooting
* Rebuilding downstream datasets

### Example

```text
Landing/
├── customers.csv
├── orders.json
└── products.xml
```

The idea is:

> **Keep the original data so that we always have a source of truth to go back to.**

---

# Step 3: ETL

ETL stands for:

* **Extract**
* **Transform**
* **Load**

## Extract

Read data from different sources.

```text
Oracle
   │
   ↓
Spark
   │
   ↓
DataFrame
```

Example:

```python
customer_df = spark.read.jdbc(...)
```

The data is extracted from the source system and loaded into a Spark DataFrame.

---

## Transform

This is where most of the data engineering work happens.

Typical transformations include:

* Remove duplicates
* Handle null values
* Standardize formats
* Rename columns
* Convert data types
* Join tables
* Create calculated columns
* Filter unwanted records

### Example

#### Raw Data

| CustomerID | Name  |
| ---------: | ----- |
|        101 | Alice |
|        101 | Alice |

#### After Transformation

| CustomerID | Name  |
| ---------: | ----- |
|        101 | Alice |

The duplicate record has been removed.

---

## Load

The transformed data is loaded into the target system.

Common targets include:

* Delta Tables
* Snowflake
* Data Warehouse
* Data Lakehouse

---

# ETL vs ELT

Nowadays, platforms such as Databricks commonly use an **ELT-oriented approach** rather than traditional ETL for many workloads.

## ETL

```text
Source
  │
  ↓
Extract
  │
  ↓
Transform
  │
  ↓
Warehouse
```

Transformation happens **before** loading the data into the target system.

---

## ELT

```text
Source
  │
  ↓
Extract
  │
  ↓
Load
  │
  ↓
Transform
  │
  ↓
Databricks / Data Warehouse / Lakehouse
```

The raw data is loaded first, and transformation happens using the processing engine.

This approach is popular because distributed engines such as **Apache Spark** are optimized for large-scale data processing.

---

# Step 4: Data Cleaning

Raw data is often messy and contains quality issues.

### Example

|  ID | Name  | Age |
| --: | ----- | --: |
| 101 | Alice |  30 |
| 101 | Alice |  30 |
| 102 | NULL  |  25 |
| 103 | Bob   |  -5 |

### Problems

The data contains:

* Duplicate rows
* Missing values
* Invalid ages

### Cleaning

We might:

* Remove duplicates
* Handle `NULL` values
* Fix or reject invalid ages
* Apply data quality rules

After cleaning:

|  ID | Name    |             Age |
| --: | ------- | --------------: |
| 101 | Alice   |              30 |
| 102 | Unknown |              25 |
| 103 | Bob     | Validated value |

The exact handling of invalid or missing data depends on the business requirements.

---

# Step 5: Data Transformation

Transformation converts raw technical data into a **business-friendly format**.

### Example 1: Date of Birth

Raw data:

| DOB        |
| ---------- |
| 1995-01-15 |

Business requirement:

| Age |
| --: |
|  30 |

The transformation calculates the customer's age from the date of birth.

### Example 2: Currency Conversion

Raw data:

| Amount |
| -----: |
|  10000 |

Transform into:

| Amount_USD |
| ---------: |
|        120 |

The exact conversion depends on the applicable exchange rate.

---

# Step 6: Data Integration

Data integration means **combining data from multiple sources or tables** to create a unified view.

### Customer Table

| CustomerID | Name  |
| ---------: | ----- |
|        101 | Alice |

### Orders Table

| OrderID | CustomerID |
| ------: | ---------: |
|    5001 |        101 |

### Join

After joining the tables:

| Customer | Order |
| -------- | ----: |
| Alice    |  5001 |

This provides a unified view of the business.

Conceptually:

```text
Customer Table
       │
       │ CustomerID
       ↓
     JOIN
       ↑
       │ CustomerID
       │
Orders Table
```

---

# Step 7: Data Validation

Before publishing data to downstream consumers, engineers verify its quality.

### Common Data Quality Checks

* Row counts
* Null values
* Duplicate keys
* Data type validation
* Business rules
* Referential integrity
* Value ranges

### Example

Suppose we want to compare row counts between the source and target.

```sql
SELECT COUNT(*)
FROM customers;
```

Example result:

```text
Oracle
  ↓
100,000 rows

Delta
  ↓
100,000 rows
```

The row counts match.

This doesn't prove that the data is completely correct, but it is one useful validation check.

---

# Step 8: Aggregation

Aggregation means **summarizing detailed data**.

## Raw Sales Data

| OrderID | Customer | Amount |
| ------: | -------- | -----: |
|       1 | Alice    |    100 |
|       2 | Alice    |    200 |
|       3 | Bob      |    150 |

Business users don't always need to see every individual order.

They may instead want total sales by customer.

## Aggregated Data

| Customer | Total Sales |
| -------- | ----------: |
| Alice    |         300 |
| Bob      |         150 |

### Spark Example

```python
sales.groupBy("Customer").sum("Amount")
```

### Common Aggregations

* `SUM`
* `COUNT`
* `AVG`
* `MIN`
* `MAX`

Aggregation is especially useful for:

* Reports
* Dashboards
* KPIs
* Business summaries
* Analytical datasets

---

# Step 9: Normalization

Normalization is mainly used in **transactional (OLTP) databases** to reduce data redundancy.

Suppose we store customer information with every order:

| OrderID | Customer | City   |
| ------: | -------- | ------ |
|       1 | Alice    | London |
|       2 | Alice    | London |

The city is repeated for every order.

## After Normalization

### Customer Table

| CustomerID | Name  | City   |
| ---------: | ----- | ------ |
|        101 | Alice | London |

### Orders Table

| OrderID | CustomerID |
| ------: | ---------: |
|       1 |        101 |
|       2 |        101 |

The orders table stores the customer's ID rather than repeating all customer information.

### Benefits

* Less duplicate data
* Easier updates
* Better data integrity
* Clearer relationships between entities

---

## Normalization vs Denormalization in Analytics

For **OLTP systems**, normalization is commonly used to reduce redundancy and maintain consistency.

For **analytics and BI**, we often **denormalize** data to reduce joins and make queries simpler and sometimes faster.

For example, instead of requiring analysts to join separate Customer and Order tables repeatedly, a reporting table might combine the relevant information into one wider table.

```text
Customer
   │
   │ JOIN
   ↓
Orders
   │
   ↓
Reporting / Analytics Table
```

This is one reason dimensional models such as **star schemas** are common in analytics.

---

# Step 10: Analytics-Ready Data — Gold Layer

The final dataset should be suitable for consumption by:

* Business users
* Data analysts
* Data scientists
* BI tools
* Machine learning applications

A Gold-layer dataset is typically:

* Clean
* Validated
* Integrated
* Business-friendly
* Aggregated where appropriate
* Optimized for reporting and analytics

### Example

| Date       | Region |     Sales |
| ---------- | ------ | --------: |
| 2025-07-01 | London | 1,250,000 |

A BI tool such as Power BI can now use this dataset to build dashboards and reports.

---

# Complete Data Engineering Flow

Putting everything together:

```text
┌───────────────────────────────────────────────────────┐
│                     DATA SOURCES                      │
│                                                       │
│ Oracle │ SAP │ REST APIs │ CSV │ Kafka │ Other APIs  │
└───────────────────────┬───────────────────────────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  1. INGESTION      │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  2. RAW / BRONZE  │
              │     LAYER         │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  3. ETL / ELT     │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  4. DATA CLEANING │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │ 5. TRANSFORMATION │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  6. INTEGRATION   │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  7. VALIDATION    │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  8. AGGREGATION   │
              └─────────┬─────────┘
                        │
                        ↓
              ┌───────────────────┐
              │  9. GOLD LAYER    │
              │ ANALYTICS-READY   │
              │      DATA         │
              └─────────┬─────────┘
                        │
          ┌─────────────┼─────────────┐
          ↓             ↓             ↓
      Power BI       Tableau          ML
          │             │             │
          └─────────────┼─────────────┘
                        ↓
                  Business Insights
```

---

# Key Takeaways

| Concept                 | Purpose                                    |
| ----------------------- | ------------------------------------------ |
| **Data Ingestion**      | Collect data from different sources        |
| **Bronze / Raw Layer**  | Preserve data as received                  |
| **ETL / ELT**           | Process and transform data                 |
| **Data Cleaning**       | Fix quality issues                         |
| **Data Transformation** | Convert data into useful business formats  |
| **Data Integration**    | Combine data from multiple sources         |
| **Data Validation**     | Verify data quality and correctness        |
| **Aggregation**         | Summarize detailed data                    |
| **Normalization**       | Reduce redundancy in transactional systems |
| **Denormalization**     | Simplify analytics and reduce joins        |
| **Gold Layer**          | Provide trusted, analytics-ready data      |

> **The core responsibility of a Data Engineer is not simply moving data from one place to another. It is building reliable pipelines that turn raw data into trustworthy, usable data for analytics and business decision-making.**
