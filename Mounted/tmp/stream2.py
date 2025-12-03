from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col
import os

# ==================================
# Paths
# ==================================
input_path = "/tmp/temp_input"
output_path = "/tmp/stream_output_json"
checkpoint_path = "/tmp/stream_checkpoint_json"

print("Current working directory:", os.getcwd())
print("Streaming from input_path:", input_path)
print("Writing JSON to:", output_path)
print("Using checkpoint path:", checkpoint_path)

# ==================================
# Spark session
# ==================================
spark = SparkSession.builder \
    .appName("StructuredStreamingJSONOutput") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==================================
# Define schema
# ==================================
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True)
])

# ==================================
# Read streaming input
# ==================================
df = spark.readStream \
    .schema(schema) \
    .json(input_path)

# ==================================
# Transformations
# ==================================
filtered_df = df.select("device_id", "temperature") \
                 .filter(col("temperature") > 70)

# ==================================
# Write JSON output
# ==================================
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()
