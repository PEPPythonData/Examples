from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import col

# -------------------------------
# 1. Create Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2. Define Schema
# -------------------------------
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),   # could also convert to TimestampType after reading
    StructField("temperature", DoubleType(), True)
])

# -------------------------------
# 3. Read Stream from Directory
# -------------------------------
input_path = "/tmp/temp_input/"

df = spark.readStream \
    .schema(schema) \
    .json(input_path)   # folder to watch

# -------------------------------
# 4. Transformations
# -------------------------------
filtered_df = df.select("device_id", "temperature") \
                .filter(col("temperature") > 70)

# -------------------------------
# 5. Write Stream to Console
# -------------------------------
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# -------------------------------
# 6. Wait for Termination
# -------------------------------
query.awaitTermination()
