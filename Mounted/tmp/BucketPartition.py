from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Step 0: Make sure warehouse dir exists inside container
warehouse_dir = "/tmp/spark-warehouse-local"
os.makedirs(warehouse_dir, exist_ok=True)

# Step 0a: Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("Partitioning and Bucketing Assignment") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", warehouse_dir) \
    .getOrCreate()

# Step 1: Create Sample DataFrame
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
df = spark.createDataFrame(data, schema=columns)
print("=== Original DataFrame ===")
df.show()

# Step 2: Partition by category and write Parquet to /tmp
partitioned_path = "/tmp/products_partitioned"
df.write.mode("overwrite").partitionBy("category").parquet(partitioned_path)
print(f"Partitioned data written to {partitioned_path}")

# Step 3: Bucket by price and save as Hive table (inside container warehouse)
spark.sql("DROP TABLE IF EXISTS products_bucketed")
df.write.mode("overwrite").bucketBy(5, "price").sortBy("price").saveAsTable("products_bucketed")
print("Bucketed table 'products_bucketed' created.")

# Step 3a: Query the bucketed table to verify
print("=== Bucketed Table Content ===")
spark.sql("SELECT * FROM products_bucketed").show()

# Step 4: Query comparison
# Query partitioned Parquet
df_partitioned = spark.read.parquet(partitioned_path)
partitioned_query = df_partitioned.filter((col("category") == "Electronics") & (col("price") > 100))
print("=== Partitioned Data Query Result ===")
partitioned_query.show()

# Query bucketed table
bucketed_query = spark.sql("SELECT * FROM products_bucketed WHERE category='Electronics' AND price > 100")
print("=== Bucketed Table Query Result ===")
bucketed_query.show()

# Stop Spark session
spark.stop()

