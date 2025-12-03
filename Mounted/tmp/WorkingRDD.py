# PySpark Transformations and Actions Assignment
from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "TransformationsAndActionsAssignment")

# -------------------------
# Task 1: Analyze the Age Data
# -------------------------
print("=== Task 1: Analyze the Age Data ===")
ages = [15, 22, 35, 42, 60, 18, 27, 19, 75, 29]

# Create an RDD from the list of ages
age_rdd = sc.parallelize(ages)

# Label each person based on age
def categorize_age(age):
    if age < 18:
        return "minor"
    elif 18 <= age <= 64:
        return "adult"
    else:
        return "senior"

age_categories_rdd = age_rdd.map(categorize_age)

# Count how many people fall into each category
category_counts = age_categories_rdd.countByValue()
print("Category counts:", category_counts)

# Get a list of all adults from the original dataset
adults = age_rdd.filter(lambda x: 18 <= x <= 64).collect()
print("Adults in the dataset:", adults)

# -------------------------
# Task 2: Process Word Data
# -------------------------
print("\n=== Task 2: Process Word Data ===")
reviews = [
    "love this product",
    "not worth the price",
    "highly recommend",
    "do not buy",
    "amazing quality",
    "very bad experience"
]

# Create an RDD of reviews
reviews_rdd = sc.parallelize(reviews)

# Count the total number of words across all reviews
words_rdd = reviews_rdd.flatMap(lambda line: line.split())
total_words = words_rdd.count()
print("Total number of words:", total_words)

# Create an RDD of (word, 1) pairs and count word frequency
word_pairs_rdd = words_rdd.map(lambda word: (word.lower(), 1))
word_freq_rdd = word_pairs_rdd.reduceByKey(lambda a, b: a + b)
word_freq = word_freq_rdd.collect()
print("Word frequencies:", word_freq)

# Filter the word frequency results to show only positive words
positive_words = ['love', 'recommend', 'amazing', 'quality', 'highly']
positive_word_freq = word_freq_rdd.filter(lambda x: x[0] in positive_words).collect()
print("Positive word frequencies:", positive_word_freq)

# -------------------------
# Task 3: Revenue Processing Challenge
# -------------------------
print("\n=== Task 3: Revenue Processing Challenge ===")
sales_data = [("hat", 25), ("shirt", 40), ("hat", 30), ("shoes", 80), ("shirt", 20)]

# Create an RDD of sales data
sales_rdd = sc.parallelize(sales_data)

# Calculate total revenue per product
total_revenue_rdd = sales_rdd.reduceByKey(lambda a, b: a + b)
total_revenue = total_revenue_rdd.collect()
print("Total revenue per product:", total_revenue)

# Identify the highest grossing product
highest_grossing = total_revenue_rdd.reduce(lambda a, b: a if a[1] > b[1] else b)
print("Highest grossing product:", highest_grossing)

# Stop SparkContext
sc.stop()
