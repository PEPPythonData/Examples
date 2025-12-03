# PySpark Transformations and Actions Assignment

## Overview

In this assignment, you'll practice using **PySpark RDD transformations** and **actions** by solving realistic data processing challenges. You’ll write your own code to complete each task based on your understanding of common transformations like `map`, `filter`, and actions like `collect`, `count`, and `reduce`.

---

## Instructions

Open a new Python script and start a Spark context as you’ve done before. Then, complete the challenges below. Try to reason about which operations are transformations and which are actions.

---


### Task 1: Analyze the Age Data

* Create an RDD from a list of ages: `[15, 22, 35, 42, 60, 18, 27, 19, 75, 29]`.
* Use a transformation to create a new RDD that labels each person as `'minor'` if under 18, `'adult'` if between 18–64, and `'senior'` if 65 or older.
* Count how many people fall into each category.
* Get a list of all adults from the original dataset.

*Hint:* Think about `map`, `filter`, and `countByValue`.

---

### Task 2: Process Word Data

You’re analyzing product reviews for sentiment. Use the following RDD of review strings:

```plaintext
["love this product", "not worth the price", "highly recommend", "do not buy", "amazing quality", "very bad experience"]
```
* Count the total number of words across all reviews.
* Create an RDD of (word, 1) pairs, then reduce them to count word frequency.
* Filter the word frequency results to show only positive words: ['love', 'recommend', 'amazing', 'quality', 'highly']

*Hint*: Use flatMap, reduceByKey, and filter.

---

### Task 3: Revenue Processing Challenge


You’re given an RDD of (product, revenue) pairs like:

```plaintext
[("hat", 25), ("shirt", 40), ("hat", 30), ("shoes", 80), ("shirt", 20)]
```

* Calculate the **total revenue** per product.
* Identify the **highest grossing** product.

*Hint:* This can be done using `reduceByKey`, followed by sorting or finding the max.

---

### Requirements

* Use a mix of transformations (`map`, `filter`, `flatMap`, etc.) and actions (`count`, `collect`, `reduce`, `take`, etc.).
* Write clean, commented PySpark code.
* Interpret the outputs after each step: What’s happening, and why?
