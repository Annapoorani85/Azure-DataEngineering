# Normalization vs Denormalization

For a **Data Engineer**, normalization and denormalization are fundamental database design concepts.

The key is understanding **when to use each**, because **OLTP systems** and **analytics systems** have different goals.

---

## 1. Normalization

**Normalization** is the process of organizing data to:

* Eliminate redundancy (duplicate data)
* Improve data consistency
* Make updates easier
* Reduce storage

The core idea is:

> **"Store each fact only once."**

### Example: Unnormalized Data

Suppose we store order information like this:

| OrderID | Customer | CustomerCity | Product | Price |
| ------: | -------- | ------------ | ------- | ----: |
|     101 | Anu      | Bangalore    | Laptop  | 70000 |
|     102 | Anu      | Bangalore    | Mouse   |   500 |

Notice that customer information is repeated:

* Anu
* Bangalore

If the customer has multiple orders, the same information appears repeatedly.

---

## 2. Normalized Data

We can split the data into multiple related tables.

### Customers

| CustomerID | Name | City      |
| ---------: | ---- | --------- |
|          1 | Anu  | Bangalore |

### Orders

| OrderID | CustomerID |
| ------: | ---------: |
|     101 |          1 |
|     102 |          1 |

### Products

| ProductID | Name   | Price |
| --------: | ------ | ----: |
|        10 | Laptop | 70000 |
|        20 | Mouse  |   500 |

### OrderItems

| OrderID | ProductID |
| ------: | --------: |
|     101 |        10 |
|     102 |        20 |

Now customer information exists **only once**.

The `Orders` table references the customer using `CustomerID`.

---

## Benefits of Normalization

### 1. No Duplicate Data

Customer information does not need to be repeated for every order.

### 2. Easier Updates

If Anu moves to Mysore, we only need to update one row:

```text
Anu → Mysore
```

### 3. Better Data Integrity

Because each fact is stored in one place, there is less chance of conflicting values.

---

## Drawback of Normalization

To answer business questions, we often need multiple joins.

For example:

```sql
Orders
JOIN Customers
JOIN Products
JOIN OrderItems
```

More joins can make analytical queries more complex and potentially slower, especially at large scale.

---

# 3. Denormalization

**Denormalization** intentionally introduces duplicate data to improve **read performance**.

Instead of storing customer, product, and order information separately, we can combine frequently accessed information.

### Example

| OrderID | Customer | City      | Product | Price |
| ------: | -------- | --------- | ------- | ----: |
|     101 | Anu      | Bangalore | Laptop  | 70000 |

Everything required for the query is already available in one row.

### Main Advantage

There are fewer joins.

This can make analytical queries faster and simpler.

---

## Benefits of Denormalization

* Very fast reads
* Fewer joins
* Simpler analytical queries
* Excellent for:

  * Dashboards
  * Reports
  * Business Intelligence (BI)
  * Data warehouses

---

## Drawbacks of Denormalization

### 1. Duplicate Data

The same customer information may appear in many rows.

### 2. More Difficult Updates

If the customer changes their city, multiple rows may need to be updated.

### 3. Possible Inconsistency

If some rows are updated and others are not, the database can contain conflicting information.

---

# 4. Simple Analogy

## Normalized

Imagine your phone contacts:

```text
Anu
 ├── Phone
 └── Address
```

The contact is stored once.

Different applications can reference the same information.

## Denormalized

Imagine copying your address into every:

* Food delivery app
* Shopping app
* Taxi app

It is convenient for each application because the information is already available.

However, if you move, you need to update your address everywhere.

---

# 5. Normal Forms

A Data Engineer doesn't usually need to memorize every rule, but should understand the common normal forms.

---

## 1NF — First Normal Form

**Rule:** No repeating groups.

### Bad

| Student | Subjects      |
| ------- | ------------- |
| Anu     | Math, Science |

Multiple subjects are stored in one column.

### Good

| Student | Subject |
| ------- | ------- |
| Anu     | Math    |
| Anu     | Science |

Each cell contains a single value.

---

## 2NF — Second Normal Form

**Rule:** Every non-key column depends on the **whole primary key**.

2NF is mainly relevant when using **composite keys**.

The goal is to remove **partial dependencies**.

---

## 3NF — Third Normal Form

**Rule:** No transitive dependency.

### Example

```text
EmpID | DeptID | DeptName
```

Here:

```text
EmpID → DeptID
DeptID → DeptName
```

`DeptName` depends on `DeptID`, not directly on `EmpID`.

Therefore, department information should be moved into a separate table.

### Better Design

**Employees**

| EmpID | DeptID |
| ----: | -----: |
|   101 |     10 |

**Departments**

| DeptID | DeptName    |
| -----: | ----------- |
|     10 | Engineering |

---

## BCNF — Boyce-Codd Normal Form

**BCNF** is a stronger version of **3NF**.

It is usually covered in more detailed database design discussions.

---

# 6. Where Data Engineers Use Normalization

## OLTP Databases

Examples:

* Banking systems
* E-commerce applications
* Hospital systems
* CRM systems

These systems perform many:

* Inserts
* Updates
* Deletes

Therefore, they generally favor **higher normalization**.

### Common OLTP Databases

* MySQL
* PostgreSQL
* SQL Server
* Oracle

### Main Goal

> **Data consistency and efficient transactional updates**

---

# 7. Where Data Engineers Use Denormalization

## Data Warehouses

Examples:

* Snowflake
* BigQuery
* Redshift
* Synapse
* Databricks

The goal is:

> **Fast analytical queries**

Data warehouses may process millions or billions of rows.

For analytical workloads, denormalized structures can reduce joins and improve query performance.

---

# 8. Star Schema

The **Star Schema** is one of the most common warehouse designs.

```text
              Product
                 |
                 |
Customer ------ Sales ------ Date
                 |
               Store
```

### Fact Table

```text
Sales
```

### Dimension Tables

```text
Customer
Product
Store
Date
```

The fact table contains measurable business events, while dimension tables provide descriptive context.

A star schema generally keeps dimensions relatively straightforward, making analytical queries easier and reducing the number of joins.

---

# 9. Snowflake Schema

In a **Snowflake Schema**, dimensions are further normalized.

Example:

```text
Sales
  |
Product
  |
Category
  |
Department
```

Compared with a star schema:

* More normalization
* More joins
* Less duplicate dimension data
* Potentially less storage
* Queries can be more complex

---

# 10. Star Schema vs Snowflake Schema

| Feature               | Star Schema        | Snowflake Schema                      |
| --------------------- | ------------------ | ------------------------------------- |
| Dimension design      | More denormalized  | More normalized                       |
| Number of joins       | Fewer              | More                                  |
| Query simplicity      | Higher             | Lower                                 |
| Data redundancy       | Higher             | Lower                                 |
| Storage               | Potentially higher | Potentially lower                     |
| Analytics performance | Generally fast     | Can be slower due to additional joins |

---

# 11. Normalization vs Denormalization

| Aspect           | Normalization          | Denormalization          |
| ---------------- | ---------------------- | ------------------------ |
| Main goal        | Reduce redundancy      | Improve read performance |
| Duplicate data   | Low                    | Higher                   |
| Data consistency | Easier to maintain     | More difficult           |
| Updates          | Easier                 | More expensive           |
| Reads            | May require more joins | Usually fewer joins      |
| Storage          | Lower                  | Potentially higher       |
| Common use       | OLTP                   | OLAP / Analytics         |

---

# 12. OLTP vs OLAP

Understanding the difference between **OLTP** and **OLAP** helps explain why normalization and denormalization are used differently.

### OLTP

```text
Frequent INSERT / UPDATE / DELETE
              ↓
      Data consistency
              ↓
       Normalization
```

Examples:

* Banking
* E-commerce transactions
* CRM applications
* Hospital applications

### OLAP

```text
Large analytical queries
              ↓
       Fast reads
              ↓
      Denormalization
```

Examples:

* Dashboards
* Reporting
* Business Intelligence
* Data warehouses

---

# 13. Interview Perspective

A common interview question is:

> **When would you normalize, and when would you denormalize?**

### Good Answer

> **Normalize** data for transactional systems where data is frequently inserted, updated, or deleted. Normalization reduces redundancy and improves consistency.
>
> **Denormalize** data for analytical systems such as data warehouses, reporting systems, and dashboards, where read performance is more important than minimizing duplicate data.

The key is understanding the **trade-off** rather than simply memorizing the definitions of every normal form.

---

# 14. Easy Mental Model

```text
NORMALIZATION
      ↓
Less duplication
      ↓
Better consistency
      ↓
More joins
      ↓
Good for OLTP


DENORMALIZATION
      ↓
More duplication
      ↓
Fewer joins
      ↓
Faster reads
      ↓
Good for OLAP / Analytics
```

## One-Line Summary

> **Normalize for write efficiency and data consistency; denormalize for read efficiency and analytical performance.**
