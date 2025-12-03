# Cache and Persist Assignment

## Overview

This assignment will help you practice using Spark’s caching and persisting mechanisms to optimize performance. You will work with a sample DataFrame, apply caching and persisting, and observe their effects during repeated actions.

---

## Instructions

### Step 1: Create a DataFrame

* Use the following sample data to create a DataFrame:

```json
[
  {"id": 1, "name": "Alice", "age": 28},
  {"id": 2, "name": "Bob", "age": 33},
  {"id": 3, "name": "Cathy", "age": 45},
  {"id": 4, "name": "David", "age": 23},
  {"id": 5, "name": "Eva", "age": 31}
]
```

* Load this data into a PySpark DataFrame.

### Step 2: Cache the DataFrame

* Cache the DataFrame using `.cache()`.
* Run an action such as `.count()` or `.collect()` to materialize the cache.
* Run the action again and observe if there is any change in performance or execution time.

### Step 3: Persist the DataFrame

* Persist the same DataFrame using `.persist()` with storage level `MEMORY_AND_DISK`.
* Run the same action again.
* Run the action a second time and observe any differences compared to caching.

---

## Requirements

* Use the provided sample data to create a DataFrame.
* Perform caching and persisting on the DataFrame.
* Execute actions to trigger evaluation of cached and persisted data.
* Observe and note differences in performance or execution.
* Include code comments to explain each step clearly.
