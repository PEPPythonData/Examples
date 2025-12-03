# Partitioning and Bucketing in PySpark – Alternative Assignment

## Overview

In this assignment, you’ll work with product sales data to practice writing partitioned and bucketed data using Hive support in PySpark. You’ll also examine how data layout can affect query performance.

## Instructions

### Step 1: Create a Sample DataFrame

Use the following sample data to construct a DataFrame:

```python
data = [
    (101, "Laptop", "Electronics", 1200, "2024-01-15"),
    (102, "Headphones", "Electronics", 200, "2024-01-17"),
    (103, "Coffee Maker", "Home", 85, "2024-01-20"),
    (104, "Desk", "Office", 300, "2024-02-01"),
    (105, "Monitor", "Electronics", 400, "2024-02-03"),
    (106, "Blender", "Home", 60, "2024-02-10"),
    (107, "Chair", "Office", 150, "2024-02-14"),
    (108, "Keyboard", "Electronics", 90, "2024-02-18"),
    (109, "Lamp", "Home", 40, "2024-02-20"),
    (110, "Notebook", "Office", 10, "2024-02-21")
]

columns = ["product_id", "product_name", "category", "price", "sale_date"]
```

Tasks:

* Create the DataFrame and show its contents.

---

### Step 2: Partition by Category

Write the DataFrame as a **partitioned** Parquet file based on the `category` column.

Tasks:

* Use `.partitionBy("category")` with `.write.mode("overwrite")`.
* Save to `products_partitioned`.

---

### Step 3: Bucket by Price

Write the same data as a table **bucketed** by the `price` column into 5 buckets.

Tasks:

* Use `.bucketBy(5, "price")` with `.sortBy("price")` and `.saveAsTable("products_bucketed")`.
* Query the table using `spark.sql()` and confirm output.

---

### Step 4: Bonus – Query Comparison

Run a query that filters products by the Electronics category and a price greater than 100 on oth the partitioned and bucketed data.

---

## Requirements

* DataFrame created from sample data.
* Partitioned data written as Parquet.
* Provide screenshots of your code and output.



