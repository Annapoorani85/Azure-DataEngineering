# Types of Data, Schema, OLTP vs OLAP, and Data Engineering

## Table of Contents

* [1. Types of Data](#1-types-of-data)

  * [Structured Data](#structured-data)
  * [Unstructured Data](#unstructured-data)
* [2. Schema](#2-schema)
* [3. OLTP vs OLAP](#3-oltp-vs-olap)

  * [OLTP](#what-is-oltp)
  * [Normalization](#normalization)
  * [Problems with OLTP for Analytics](#problems-with-oltp-for-analytics)
  * [OLAP](#what-is-olap)
  * [OLTP vs OLAP Comparison](#oltp-vs-olap-comparison)
* [4. Why Do We Need OLAP?](#4-why-do-we-need-olap)
* [5. How Data Engineers Build OLAP Systems](#5-how-data-engineers-build-olap-systems)
* [6. ETL Example](#6-etl-example)
* [7. Aggregation](#7-aggregation)
* [8. Where Does Databricks Fit?](#8-where-does-databricks-fit)
* [9. Interview Answer](#9-interview-answer)
* [10. Key Takeaways](#10-key-takeaways)

---

# 1. Types of Data

## Structured Data

**Structured data** is information organized according to a **predefined format or schema**, so computers can easily store, search, sort, and analyze it.

Examples include:

* SQL database tables
* Spreadsheets with consistent columns
* Transaction records

### Simple Definition

> **Structured data = data organized into clearly defined fields according to a schema.**

Example:

| Student ID | Name | Age | Email                                       |
| ---------: | ---- | --: | ------------------------------------------- |
|        101 | Anu  |  21 | [anu@example.com](mailto:anu@example.com)   |
|        102 | Ravi |  22 | [ravi@example.com](mailto:ravi@example.com) |

The structure is clearly defined:

```text
Student ID → INTEGER
Name       → STRING
Age        → INTEGER
Email      → STRING
```

---

## Unstructured Data

**Unstructured data** is information that does not follow a predefined structure or fixed format.

It is usually harder for traditional databases to organize and analyze directly.

### Examples

* **Documents** — Word files, PDFs, reports
* **Emails and messages** — free-form conversations
* **Images** — photos, X-rays, scanned documents
* **Videos** — movies, CCTV footage, interviews
* **Audio** — voice recordings, podcasts
* **Social media posts** — text, images, videos, comments

---

# 2. Schema

A **schema** is the structure or blueprint that describes:

* How data is organized
* What each field represents
* What type of value each field should contain
* Which fields are required or optional
* What rules apply to the data

## Example

Consider a student dataset:

| student_id | name | age | email                                       | joining_date |
| ---------: | ---- | --: | ------------------------------------------- | ------------ |
|        101 | Anu  |  21 | [anu@example.com](mailto:anu@example.com)   | 2026-06-01   |
|        102 | Ravi |  22 | [ravi@example.com](mailto:ravi@example.com) | 2026-06-05   |

The schema tells us:

```text
student_id   → INTEGER
name         → STRING
age          → INTEGER
email        → STRING
joining_date → DATE
```

### Data vs Schema

A useful way to remember the difference:

```text
Data
= Actual values

Schema
= Rules / structure describing those values
```

---

## What Does a Schema Contain?

A schema can contain:

1. **Column / field name**
2. **Data type** — what kind of value can go into the column
3. **Whether the value is required or optional**
4. **Description of the field**
5. **Constraints and relationships**, depending on the system

For example:

```text
revenue

Type:
NUMERIC

Description:
Total transaction amount in INR
```

In an RDBMS, the term **schema** can describe more than just columns and data types. It can also describe the logical structure and rules of the database.

For example:

```text
student_id → PRIMARY KEY

course_id → FOREIGN KEY
```

---

## What Happens When Data Doesn't Match the Schema?

When incoming data doesn't match the expected schema, it can result in:

* Ingestion failure
* Conversion errors
* Rejected records
* `NULL` values, depending on the system/process
* Incorrect data if conversion is handled poorly

This is why **schema management and data validation** are important parts of data engineering.

---

# 3. OLTP vs OLAP

Understanding **OLTP vs OLAP** explains why organizations build data warehouses, lakehouses, and ETL/ELT pipelines in the first place.

---

# What is OLTP?

**OLTP = Online Transaction Processing**

The primary purpose of OLTP is to **run the business** by processing day-to-day transactions.

### Examples

* Amazon order placement
* ATM cash withdrawal
* Online banking
* Flight booking
* Hospital patient registration

---

## Example: Online Shopping

When a customer buys a laptop, several operations may happen:

```text
Customer places an order
        ↓
Update Inventory
        ↓
Process Payment
        ↓
Create Order Record
        ↓
Send Confirmation
```

Every transaction needs to be:

* Fast ⚡
* Accurate ✅
* Reliable 🔒

---

# OLTP Database Example

A typical OLTP system may have multiple related tables.

## Customers

| CustomerID | Name  |
| ---------: | ----- |
|        101 | Alice |

## Orders

| OrderID | CustomerID | ProductID |
| ------: | ---------: | --------: |
|    5001 |        101 |       201 |

## Products

| ProductID | Product | Price |
| --------: | ------- | ----: |
|       201 | Laptop  |  1200 |

Notice that the data is split across multiple tables.

This is called **normalization**.

---

# Normalization

Normalization is commonly used in OLTP databases to reduce data redundancy and improve consistency.

Suppose Alice buys 100 products.

Without normalization, customer information could be repeated:

| Customer | Address | Product  |
| -------- | ------- | -------- |
| Alice    | London  | Laptop   |
| Alice    | London  | Mouse    |
| Alice    | London  | Keyboard |

The value `"London"` is repeated many times.

---

## Normalized Design

### Customer Table

| CustomerID | Address |
| ---------: | ------- |
|        101 | London  |

### Orders Table

| OrderID | CustomerID |
| ------: | ---------: |
|       1 |        101 |

Now the customer's address is stored once.

### Benefits

* Less duplicate data
* Faster updates
* Better consistency
* Better data integrity

---

# Problems with OLTP for Analytics

Suppose the CEO asks:

> **"Show total sales for every country over the last five years."**

The data may be distributed across many tables.

The database might need to:

```text
Join Customers
      ↓
Join Orders
      ↓
Join Products
      ↓
Join Payments
      ↓
Join Shipping
      ↓
Aggregate historical data
```

These operations can be expensive.

The problem becomes even more significant when the same database is simultaneously processing thousands of customer transactions.

A large analytical query could negatively impact the operational workload.

---

# What is OLAP?

**OLAP = Online Analytical Processing**

The purpose of OLAP is **analysis**, not transaction processing.

Instead of placing orders, users ask analytical questions such as:

* Total sales by month
* Top-selling products
* Revenue by region
* Customer retention
* Profit trends

---

# OLAP Database

Instead of using many highly normalized transactional tables, we create an **analytics-friendly model**.

For example:

## Sales Fact

| Date | Customer | Product | Sales |
| ---- | -------- | ------- | ----: |
| Jan  | Alice    | Laptop  |  1200 |

## Product Dimension

| ProductID | Category    |
| --------: | ----------- |
|       201 | Electronics |

## Customer Dimension

| CustomerID | Country |
| ---------: | ------- |
|        101 | UK      |

This type of structure is optimized for reporting and analytics.

---

# OLTP vs OLAP Comparison

| Feature           | OLTP                                    | OLAP                                  |
| ----------------- | --------------------------------------- | ------------------------------------- |
| **Purpose**       | Run business transactions               | Analyze business data                 |
| **Users**         | Customers, employees, operational users | Analysts, managers, data scientists   |
| **Operations**    | Insert, Update, Delete                  | Read, Aggregate                       |
| **Schema**        | Normalized                              | Often denormalized / Star / Snowflake |
| **Queries**       | Usually simpler                         | Often complex                         |
| **Response Time** | Milliseconds                            | Seconds/minutes may be acceptable     |
| **Data**          | Mostly current operational data         | Historical + current data             |
| **Workload**      | Transaction processing                  | Analytical processing                 |
| **Examples**      | Oracle, MySQL                           | Snowflake, Databricks, Redshift       |

---

# 4. Why Do We Need OLAP?

Imagine an e-commerce company has:

* 50 million customers
* 500 million orders
* 2 billion order items

At the same time, the website is processing thousands of purchases every second.

Now the CEO asks:

> **"What were the top 20 products sold in Europe during the last 3 years?"**

Running this large analytical query directly against the OLTP database could negatively affect customer transactions.

Instead, data is copied and transformed into an OLAP system designed specifically for analytics.

This separates:

```text
Operational Workload
        │
       OLTP

        vs.

Analytical Workload
        │
       OLAP
```

This separation is a key reason organizations build **data warehouses and lakehouses**.

---

# 5. How Data Engineers Build OLAP Systems

This is where **Data Engineers** come in.

A simplified architecture looks like:

```text
OLTP Database
     │
     │ Extract
     ↓
ETL / ELT Pipeline
     │
     │ Clean
     │ Transform
     │ Join
     │ Validate
     │ Aggregate
     ↓
Data Warehouse / Lakehouse
     │
     ↓
OLAP Dataset
     │
     ↓
Power BI / Tableau
```

For example:

```text
Oracle
  │
  ↓
Bronze — Raw Data
  │
  ↓
Silver — Cleaned & Joined
  │
  ↓
Gold — Aggregated Analytics Tables
  │
  ↓
Power BI
```

---

# 6. ETL Example

Suppose our OLTP database contains:

## Customers

|  ID | Name  |
| --: | ----- |
| 101 | Alice |

## Orders

| OrderID | CustomerID | Amount |
| ------: | ---------: | -----: |
|       1 |        101 |    100 |
|       2 |        101 |    200 |

---

## ETL Process

A Data Engineering pipeline could:

1. Extract data from Oracle
2. Join Customers and Orders
3. Clean missing values
4. Convert currencies if required
5. Calculate total sales
6. Validate the result
7. Load the analytics-ready data into the target platform

---

## OLAP Table

After transformation and aggregation:

| Customer | Total Sales |
| -------- | ----------: |
| Alice    |         300 |

Now Power BI can use this table directly without repeatedly performing expensive joins across the operational database.

---

# 7. Aggregation

Business users usually don't need every individual transaction.

For example, raw data may look like:

| Order | Customer | Amount |
| ----: | -------- | -----: |
|     1 | Alice    |    100 |
|     2 | Alice    |    200 |
|     3 | Alice    |    150 |

Instead, we can create an aggregated table:

| Customer | Total Sales |
| -------- | ----------: |
| Alice    |         450 |

The aggregation can be performed during the data pipeline.

For example, in Spark:

```python
sales.groupBy("Customer").sum("Amount")
```

Aggregation makes analytical queries simpler and can improve reporting performance.

---

# 8. Where Does Databricks Fit?

Databricks is commonly used to build the **analytical processing layer** in modern data platforms.

A simplified architecture is:

```text
Oracle / OLTP
      │
      ↓
Bronze
Raw Data
      │
      ↓
Silver
Cleaned + Joined Data
      │
      ↓
Gold
Aggregated Analytics Tables
      │
      ↓
Power BI / Tableau
```

The **Gold layer** contains analytics-ready data and can serve as the curated dataset consumed by reporting and analytical workloads.

---

# 9. Interview Answer

### Q: Why can't we use OLTP directly for reporting?

A strong answer is:

> **OLTP systems are optimized for fast transactional operations such as inserts, updates, and deletes. They commonly use normalized schemas to reduce redundancy and maintain consistency. However, analytical queries often require large joins and aggregations over historical data, which can negatively impact transaction performance. To avoid this, we extract data from OLTP systems, transform and clean it using ETL or ELT pipelines, and load it into an OLAP system such as a data warehouse or lakehouse. The OLAP system is optimized for reporting, aggregation, trend analysis, and business intelligence.**

---

# 10. Key Takeaways

## Data Types

```text
Structured Data
    ↓
Defined schema
    ↓
Tables / spreadsheets / transactions

Unstructured Data
    ↓
No fixed predefined structure
    ↓
Documents / images / videos / audio / messages
```

## Schema

```text
Schema
   ↓
Structure + Rules
   ↓
Column Names
Data Types
Constraints
Relationships
```

## OLTP

```text
OLTP
 ↓
Run the business
 ↓
Transactions
 ↓
Fast INSERT / UPDATE / DELETE
 ↓
Normalized data
```

## OLAP

```text
OLAP
 ↓
Analyze the business
 ↓
Reporting / BI
 ↓
Large reads + aggregations
 ↓
Historical + current data
```

## Data Engineering

```text
OLTP
  ↓
Extract
  ↓
ETL / ELT
  ↓
Clean
  ↓
Transform
  ↓
Join
  ↓
Validate
  ↓
Aggregate
  ↓
OLAP / Warehouse / Lakehouse
  ↓
Power BI / Tableau / Analytics
```

---

# Final Mental Model

The easiest way to understand the whole concept is:

```text
                  OPERATIONAL WORLD
                         │
                         ↓
                 ┌───────────────┐
                 │     OLTP      │
                 │               │
                 │ Run the       │
                 │ business      │
                 │               │
                 │ Transactions  │
                 └───────┬───────┘
                         │
                       Extract
                         │
                         ↓
                 ┌───────────────┐
                 │ ETL / ELT     │
                 │               │
                 │ Clean         │
                 │ Transform     │
                 │ Join          │
                 │ Validate      │
                 │ Aggregate     │
                 └───────┬───────┘
                         │
                         ↓
                 ┌───────────────┐
                 │ OLAP          │
                 │               │
                 │ Analyze the   │
                 │ business      │
                 └───────┬───────┘
                         │
                         ↓
              ┌──────────────────────┐
              │ Power BI / Tableau   │
              │ Reports / Analytics  │
              └──────────────────────┘
```

### Core Data Engineering Responsibility

> **As a Data Engineer, your job is to build reliable ETL/ELT pipelines that move data from operational OLTP systems into analytical platforms, transforming raw transactional data into clean, trusted, and analytics-ready datasets without negatively impacting day-to-day business operations.**
