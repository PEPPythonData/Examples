# Shared Variables Assignment - Complete Solution with Output File

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import time
import sys

# Redirect print to file
output_file = "/tmp/assignment_output.txt"
sys.stdout = open(output_file, "w")

# -------------------------------
# Step 1: Setup and Data Preparation
# -------------------------------

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SharedVariablesAssignment") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Generate sample e-commerce transaction data
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

# Create DataFrame
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
print("Sample transactions:")
df.show(10, truncate=False)
print(f"Total transactions: {df.count()}\n")

# -------------------------------
# Step 2: Broadcast Variables
# -------------------------------

# Reference data
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

# Create broadcast variables
broadcast_discounts = spark.sparkContext.broadcast(category_discounts)
broadcast_tax_rates = spark.sparkContext.broadcast(region_tax_rates)

# Approach 1: Using map with broadcast variables
def calculate_final_price(row):
    discount = broadcast_discounts.value.get(row.product_category, 0)
    tax = broadcast_tax_rates.value.get(row.region, 0)
    final_price = row.amount * (1 - discount) * (1 + tax)
    return (row.transaction_id, row.customer_id, row.product_category, row.region, row.amount, row.quantity, row.transaction_date, final_price)

start_time = time.time()
rdd_result = df.rdd.map(calculate_final_price)
rdd_df = rdd_result.toDF(["transaction_id","customer_id","product_category","region","amount","quantity","transaction_date","final_price"])
rdd_df.show(10, truncate=False)
broadcast_time = time.time() - start_time
print(f"Time using broadcast variables: {broadcast_time:.4f} seconds\n")

# Approach 2: Using DataFrame joins
# Convert dictionaries to DataFrames
discount_df = spark.createDataFrame(list(category_discounts.items()), ["product_category", "discount_rate"])
tax_df = spark.createDataFrame(list(region_tax_rates.items()), ["region", "tax_rate"])

start_time = time.time()
df_joined = df.join(discount_df, on="product_category", how="left") \
              .join(tax_df, on="region", how="left") \
              .withColumn("final_price", col("amount") * (1 - col("discount_rate")) * (1 + col("tax_rate")))
df_joined.show(10, truncate=False)
join_time = time.time() - start_time
print(f"Time using DataFrame joins: {join_time:.4f} seconds\n")

# -------------------------------
# Step 3: Accumulators
# -------------------------------

# Create accumulators
total_amount_acc = spark.sparkContext.accumulator(0.0)
electronics_count_acc = spark.sparkContext.accumulator(0)

# Function to process each row and update accumulators
def process_row(row):
    global total_amount_acc, electronics_count_acc
    total_amount_acc += row.amount
    if row.product_category == "Electronics":
        electronics_count_acc += 1

# Apply function to each row
df.foreach(process_row)

# Print final accumulator values
print(f"Total amount across all transactions: {total_amount_acc.value}")
print(f"Number of Electronics transactions: {electronics_count_acc.value}")

# Stop Spark session
spark.stop()
print(f"\nAll outputs have been written to '{output_file}'.")
