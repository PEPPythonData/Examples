from pyspark.sql import SparkSession

import os

os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

spark = SparkSession.builder \
   .appName("Local PySpark") \
   .master("local[*]") \
   .getOrCreate()

# spark = SparkSession.builder.appName("example-pyspark") \
#     .master("spark://127.0.0.1:7077") \
#     .config("spark.driver.host", "127.0.0.1") \
#     .config("spark.driver.bindAddress", "0.0.0.0") \
#     .getOrCreate()


# spark = SparkSession.builder.appName("example-pyspark-hdfs")\
# .config("spark.master", "spark://10.89.0.4:7077")\
# .config("spark.driver.host", "127.0.0.1") \
# .config("spark.driver.bindAddress", "0.0.0.0") \
# .getOrCreate()

#spark = SparkSession.builder \
#    .appName("Local PySpark") \
#    .master("spark://localhost:7077") \
#    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

#Test your Spark session
df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
df.show()

data = [("John", 28), ("Jane", 25)]
columns = ["Name", "Age"]


df2 = spark.createDataFrame(data, columns)
df2.show()
df3=df2.coalesce(1)

df3.write.format("csv").mode("overwrite").save("file:///opt/spark/work-dir")


# Get the SparkContext from the SparkSession
sc = spark.sparkContext

# Create an RDD from a collection (e.g., a list)
dataRDD = [1, 2, 3, 4, 5]
rdd = sc.parallelize(dataRDD)

# Perform an action to verify the RDD
print(rdd.collect())  # Output: [1, 2, 3, 4, 5]

