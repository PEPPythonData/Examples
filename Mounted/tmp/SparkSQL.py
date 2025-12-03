from pyspark.sql import SparkSession

# Step 0: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Spark SQL Assignment") \
    .getOrCreate()

# Step 1: Load and Prepare the Data
# Assuming orders.csv is in the same directory as this script
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)

# Register the DataFrame as a temporary view called 'orders'
orders_df.createOrReplaceTempView("orders")

# Step 2: Basic SQL Queries

# 2.1: Select all records from the orders view
print("All orders:")
spark.sql("SELECT * FROM orders").show()

# 2.2: Return only orders where the status is 'shipped'
print("Orders with status 'shipped':")
spark.sql("SELECT * FROM orders WHERE status = 'shipped'").show()

# 2.3: Find the total order amount for each customer
print("Total order amount per customer:")
spark.sql("""
    SELECT customer, SUM(amount) AS total_amount
    FROM orders
    GROUP BY customer
""").show()

# Step 3: Filtering and Ordering

# 3.1: Show all orders over $100, sorted by amount descending
print("Orders over $100, sorted by amount descending:")
spark.sql("""
    SELECT *
    FROM orders
    WHERE amount > 100
    ORDER BY amount DESC
""").show()

# 3.2: List customers who have more than one order
print("Customers with more than one order:")
spark.sql("""
    SELECT customer, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer
    HAVING COUNT(*) > 1
""").show()

# Step 4: Filtering and Aggregation

# 4.1: Top 3 customers by total order amount
print("Top 3 customers by total order amount:")
spark.sql("""
    SELECT customer, SUM(amount) AS total_amount
    FROM orders
    GROUP BY customer
    ORDER BY total_amount DESC
    LIMIT 3
""").show()

# Stop the Spark session
spark.stop()
