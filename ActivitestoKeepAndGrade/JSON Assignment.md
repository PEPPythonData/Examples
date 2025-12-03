# Working with JSON Files in PySpark

## Overview

In this assignment, you will practice loading data from a JSON file into a PySpark DataFrame, inspecting the schema, working with nested or structured data, and saving it to a new JSON file.

## Instructions

### Step 1: Prepare the JSON File

Copy and paste the following into a file named `employees.json`:

```json
[
  {
    "id": 1,
    "name": "Alice",
    "department": "HR",
    "salary": 50000
  },
  {
    "id": 2,
    "name": "Bob",
    "department": "Engineering",
    "salary": 75000
  },
  {
    "id": 3,
    "name": "Charlie",
    "department": "Finance",
    "salary": 62000
  },
  {
    "id": 4,
    "name": "Dana",
    "department": "Engineering",
    "salary": 80000
  }
]
```

Save this file to a directory your Spark session can access.

---

### Step 2: Read JSON Data

Use PySpark to load the JSON file into a DataFrame.

Tasks:

* Load the `employees.json` file using `.read.json()`
* Display the schema using `.printSchema()`
* Preview the data using `.show()`

---

### Step 3: Explore Schema and Null Handling

Investigate how Spark handles missing or unexpected fields.

Tasks:

* Manually add a fifth employee to your JSON file with a missing `salary` field and re-read the file.
* Observe how Spark represents the missing data in the DataFrame.
* Use `.na.fill()` to assign a default salary (e.g., 40000) to any rows with a missing `salary` value.
* Display the updated DataFrame.

---

### Step 4: Write Data to JSON

Save the modified DataFrame to a new file.

Tasks:

* Write the DataFrame to a file named `employees_cleaned.json`
* Use `mode("overwrite")` to avoid issues with repeated runs

---

### Requirements

* Read from and write to JSON using Spark
* Inspect schema and handle null values
* Apply basic DataFrame methods such as `.read`, `.show`, `.na.fill`, and `.write`
