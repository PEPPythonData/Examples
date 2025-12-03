# PySpark DataFrames Assignment Solution

# Import necessary PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when, count

# Step 0: Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark DataFrames Assignment") \
    .getOrCreate()

# Step 1: Load and Explore the Data
# ----------------------------------
# Load the CSV file into a DataFrame
df = spark.read.csv("people.csv", header=True, inferSchema=True)

# Show first few rows
print("First few rows of the DataFrame:")
df.show()

# Print the schema to see column types
print("Schema of the DataFrame:")
df.printSchema()

# Count total number of records
total_records = df.count()
print(f"Total number of records: {total_records}\n")

# Step 2: Filtering and Selecting Columns
# ---------------------------------------
# Filter for people from New York
ny_people = df.filter(col("city") == "New York")

# Select only name and salary columns
ny_people_selected = ny_people.select("name", "salary")

# Show the filtered DataFrame
print("People from New York (name and salary):")
ny_people_selected.show()

# Count number of people from New York
ny_count = ny_people.count()
print(f"Number of people from New York: {ny_count}")

# Calculate average salary for people from New York
ny_avg_salary = ny_people.agg(avg("salary")).collect()[0][0]
print(f"Average salary of people from New York: {ny_avg_salary}\n")

# Step 3: Grouping and Aggregation
# --------------------------------
# Group by city and calculate average salary
city_avg_salary = df.groupBy("city").agg(avg("salary").alias("avg_salary"))

# Sort by average salary descending
city_avg_salary_sorted = city_avg_salary.orderBy(col("avg_salary").desc())

print("Average salary per city (sorted descending):")
city_avg_salary_sorted.show()

# Step 4: DataFrame Transformation
# --------------------------------
# Add a new column 'income_bracket' based on salary
df_with_bracket = df.withColumn(
    "income_bracket",
    when(col("salary") < 80000, "Low")
    .when((col("salary") >= 80000) & (col("salary") < 100000), "Mid")
    .otherwise("High")
)

print("DataFrame with income_bracket column:")
df_with_bracket.show()

# Count how many people fall into each income bracket
income_bracket_counts = df_with_bracket.groupBy("income_bracket").count()

print("Number of people in each income bracket:")
income_bracket_counts.show()

# Stop the SparkSession
spark.stop()
