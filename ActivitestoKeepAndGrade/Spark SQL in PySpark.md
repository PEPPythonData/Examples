# Spark SQL in PySpark Assignment

## Overview

In this assignment, you'll practice writing SQL queries in PySpark using `spark.sql()`. You'll load a dataset into a DataFrame, register it as a temporary view, and perform analytical queries using SQL syntax.

---

## Instructions

Create a CSV file called `orders.csv` with the following information:

```csv
order_id,customer,amount,status
1,Alice,120.50,shipped
2,Bob,75.00,pending
3,Cathy,250.00,shipped
4,Alice,300.00,pending
5,David,50.00,shipped
6,Bob,150.00,shipped
```

---

### Step 1: Load and Prepare the Data

* Load the CSV file into a DataFrame.
* Register the DataFrame as a temporary view called `orders`.

 *Key API:* `createOrReplaceTempView("orders")`

---

### Step 2: Write Basic SQL Queries


Using `spark.sql()`, write SQL queries to:

1. Select all records from the `orders` view.
2. Return only orders where the `status` is `'shipped'`.
3. Find the total order amount for each customer.

*Think about:* `SELECT *`, `WHERE`, `GROUP BY`, `SUM()`

---

### Step 3: Filtering and Ordering

Write SQL queries to:

1. Show all orders over \$100, sorted by amount descending.
2. List customers who have more than one order.

*Think about:* `HAVING`, `COUNT(*)`, `ORDER BY`

---

### Step 4: Filtering and Aggregation

Write a SQL query that answers the following:
- What are the top 3 customers by total order amount?

Your query should:

* Group orders by customer_name
* Calculate the total amount ordered by each customer
* Sort the results in descending order of total amount
* Return only the top 3 customers

*Think about*: `GROUP BY`, `SUM()`, `ORDER BY`, `LIMIT`

---

## Requirements

* You must use **only SQL syntax** via `spark.sql()` (no DataFrame API).
* Include comments in your notebook or script explaining what each query does.
* Include `show()` outputs to verify query results.
