# DataFrames in PySpark Assignment

## Overview

In this assignment, you'll work with **PySpark DataFrames** to practice common tasks like reading structured data, exploring schema, performing basic transformations, and aggregating results. These are foundational skills used in real-world data engineering and analysis.

---

## Instructions

You’ve been given a CSV file named `people.csv` with the following sample structure:

```csv
name,age,city,salary
Alice,30,New York,85000
Bob,25,San Francisco,90000
Cathy,40,New York,110000
David,23,Seattle,70000
Eva,35,San Francisco,95000
```

Assume this file is already available in your local directory. Begin by creating a **SparkSession** and loading this CSV into a DataFrame.

---

### Step 1: Load and Explore the Data

* Load the CSV file as a DataFrame using `spark.read`.
* Show the first few rows and print the schema.
* Count how many records are in the dataset.

*Think about:* `show()`, `printSchema()`, `count()`

---

### Step 2: Filtering and Selecting Columns

* Create a new DataFrame containing only people from New York.
* From this filtered DataFrame, select only the `name` and `salary` columns.
* How many people are from New York, and what is their average salary?

*Think about:* `filter()`, `select()`, `agg()`

---

### Step 3: Grouping and Aggregation

* Group the data by `city` and calculate the average salary per city.
* Sort the results in descending order of average salary.

*Think about:* `groupBy()`, `avg()`, `orderBy()`

---

### Step 4: DataFrame Transformation

* Add a new column `income_bracket`:

  * `'Low'` if salary < 80,000
  * `'Mid'` if 80,000 ≤ salary < 100,000
  * `'High'` if salary ≥ 100,000
* Count how many people fall into each income bracket.

*Think about:* `withColumn()`, `when()`, `count()` or `groupBy()` + `count()`

---

## Requirements

* Use only **PySpark DataFrame** APIs (not RDD).
* Include appropriate imports and initialize a `SparkSession`.
* Comment your code to explain what each transformation or action is doing.

