from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def main():
    spark = SparkSession.builder.appName("Employees JSON Processing").getOrCreate()

    # Explicit schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])

    # Read JSON with schema
    df = spark.read.option("multiLine", True).schema(schema).json("employees.json")

    print("=== Original Data ===")
    df.show()
    df.printSchema()

    # Fill missing salaries
    df_filled = df.na.fill({"salary": 40000})

    print("=== Data After Filling Missing Salaries ===")
    df_filled.show()

    # Write single JSON file
    df_filled.coalesce(1).write.mode("overwrite").json("/tmp/employees_cleaned.json")

    spark.stop()

if __name__ == "__main__":
    main()
