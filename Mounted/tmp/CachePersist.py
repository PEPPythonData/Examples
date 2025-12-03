from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time

# Step 0: Create Spark session
spark = SparkSession.builder \
    .appName("Cache and Persist Assignment") \
    .getOrCreate()

# Step 1: Create a DataFrame with sample data
data = [
    {"id": 1, "name": "Alice", "age": 28},
    {"id": 2, "name": "Bob", "age": 33},
    {"id": 3, "name": "Cathy", "age": 45},
    {"id": 4, "name": "David", "age": 23},
    {"id": 5, "name": "Eva", "age": 31}
]

df = spark.createDataFrame(data)

print("Original DataFrame:")
df.show()

# Step 2: Cache the DataFrame
print("\nCaching the DataFrame...")
df.cache()  # Marks the DataFrame to be cached in memory

# Trigger an action to materialize the cache
start_time = time.time()
count_cached = df.count()
end_time = time.time()
print(f"Count after caching: {count_cached}, Time taken: {end_time - start_time:.6f} seconds")

# Run the action again to see performance improvement
start_time = time.time()
count_cached_again = df.count()
end_time = time.time()
print(f"Count after caching (2nd run): {count_cached_again}, Time taken: {end_time - start_time:.6f} seconds")

# Step 3: Persist the DataFrame with MEMORY_AND_DISK
print("\nPersisting the DataFrame with MEMORY_AND_DISK...")
df.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger an action to materialize the persisted DataFrame
start_time = time.time()
count_persisted = df.count()
end_time = time.time()
print(f"Count after persisting: {count_persisted}, Time taken: {end_time - start_time:.6f} seconds")

# Run the action again to observe performance
start_time = time.time()
count_persisted_again = df.count()
end_time = time.time()
print(f"Count after persisting (2nd run): {count_persisted_again}, Time taken: {end_time - start_time:.6f} seconds")

# Optional: Unpersist the DataFrame to free memory
df.unpersist()

# Stop the Spark session
spark.stop()
