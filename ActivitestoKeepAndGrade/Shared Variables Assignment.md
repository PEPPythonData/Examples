# Shared Variables Assignment

## Overview

In this assignment, you will explore PySpark's shared variables: **broadcast variables** and **accumulators**. These are essential tools for efficient distributed computing, allowing you to share read-only data across all nodes (broadcast variables) and safely aggregate values from executors back to the driver (accumulators).

You'll work with a dataset of e-commerce transactions to implement real-world scenarios where shared variables provide significant performance benefits.

## Instructions

### Step 1: Setup and Data Preparation

Create a new PySpark session and generate sample e-commerce data:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SharedVariablesAssignment") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Sample data generation
def generate_transaction_data(num_records=10000):
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"]
    regions = ["North", "South", "East", "West", "Central"]
    
    data = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        transaction_id = f"TXN_{i:06d}"
        customer_id = f"CUST_{random.randint(1, 2000):05d}"
        product_category = random.choice(categories)
        region = random.choice(regions)
        amount = round(random.uniform(10.0, 500.0), 2)
        quantity = random.randint(1, 5)
        transaction_date = base_date + timedelta(days=random.randint(0, 365))
        
        data.append((transaction_id, customer_id, product_category, region, 
                    amount, quantity, transaction_date))
    
    return data

# Create the dataset
transaction_data = generate_transaction_data()
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", DateType(), True)
])

df = spark.createDataFrame(transaction_data, schema)
df.cache()
df.show(10)
print(f"Total transactions: {df.count()}")
```

### Step 2: Implement Broadcast Variables

Create and use broadcast variables to efficiently add reference data to your DataFrame:

```python
# Create reference data that would typically come from external sources
category_discounts = {
    "Electronics": 0.10,
    "Clothing": 0.15,
    "Books": 0.05,
    "Home": 0.08,
    "Sports": 0.12,
    "Beauty": 0.20
}

region_tax_rates = {
    "North": 0.08,
    "South": 0.06,
    "East": 0.09,
    "West": 0.07,
    "Central": 0.05
}

# TODO: Create broadcast variables for the dictionaries above
# broadcast_discounts = spark.sparkContext.broadcast(?)
# broadcast_tax_rates = spark.sparkContext.broadcast(?)

# TODO: Convert dictionaries to DataFrames for joining
# Create a DataFrame from category_discounts with columns: category, discount_rate
# Create a DataFrame from region_tax_rates with columns: region, tax_rate

# TODO: Join the main DataFrame with both reference DataFrames
# Use left joins to add discount_rate and tax_rate columns

# TODO: Calculate final_price using the formula:
# final_price = amount * (1 - discount_rate) * (1 + tax_rate)

# TODO: Compare the performance difference between:
# 1. Using broadcast variables with map() operations
# 2. Using regular DataFrame joins
# Time both approaches and note the difference
```

### Step 3: Implement Accumulators

Use accumulators to track various metrics during data processing:

```python
# TODO: Create accumulators for tracking different metrics


# TODO: Create a function that processes each row and updates accumulators


# TODO: Apply the function to each row using foreach


# TODO: Print the final accumulator values


```

### Requirements

- Demonstrate accurate use of shared variables
- code and output provided in a text file
