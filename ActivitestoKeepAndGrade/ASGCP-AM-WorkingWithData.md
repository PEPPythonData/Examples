# Assignment: Spark ETL with Google Cloud Storage and Dataproc

## **Overview**

In this assignment, you will simulate a simple **ETL (Extract, Transform, Load)** process using **Apache Spark** on **Google Cloud Dataproc**. The job will:

* Extract raw data from **Google Cloud Storage (GCS)**
* Perform a transformation using Spark
* Load the results back into a different GCS bucket

You’ll learn how to:

* Integrate Spark jobs with GCS
* Submit Spark jobs to a Dataproc cluster
* Use Dataproc to manage data pipelines in the cloud

## **Instructions**

### **Step 1: Prepare Your GCS Buckets**

1. Open [https://console.cloud.google.com/storage](https://console.cloud.google.com/storage)
2. Create **two buckets**:

   * One for raw input data: `your-bucket-name-input`
   * One for transformed output data: `your-bucket-name-output`
3. Upload a sample CSV file to the input bucket. For example:

   ```csv
   name,age,city
   Alice,30,New York
   Bob,25,Chicago
   Charlie,35,San Francisco
   ```

### **Step 2: Write a Spark ETL Job (Python or Scala)**

Create a Spark job that:

* **Reads** the CSV file from the GCS input bucket
* **Filters** the data to include only rows where age > 28
* **Writes** the result back to the output bucket in CSV or Parquet format

Example (Python):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("GCS ETL").getOrCreate()

# Read from GCS
df = spark.read.csv("gs://your-bucket-name-input/data.csv", header=True, inferSchema=True)

# Transformation
filtered_df = df.filter(df.age > 28)

# Write to GCS
filtered_df.write.mode("overwrite").csv("gs://your-bucket-name-output/filtered")
```

Save this file as `etl_job.py`.

### **Step 3: Upload Your Spark Job to GCS**

* Upload `etl_job.py` to a GCS bucket (e.g., `gs://your-bucket-name-input/scripts/etl_job.py`)

### **Step 4: Submit the Spark Job to Dataproc**

1. Open the GCP Console and navigate to **Dataproc > Jobs**
2. Submit a new job:

   * **Cluster**: Select your existing Dataproc cluster
   * **Job Type**: PySpark
   * **Main Python file**: `gs://your-bucket-name-input/scripts/etl_job.py`
3. Click **Submit** and monitor the job's progress

### **Step 5: Verify the Output**

* After the job completes, go to your **output bucket**
* Verify that a folder called `filtered/` has been created
* Download and inspect the resulting files

## **Requirements**

* A Spark job must be written to:

  * Read data from a GCS bucket
  * Filter the data (age > 28)
  * Write the transformed result back to a different GCS bucket
* The job must be submitted to Dataproc and successfully executed
* The resulting output files must be present and valid


## Stretch Goals (Optional Enhancements)

1. **Use Parquet Format**

   * Modify your job to write output in `.parquet` format for optimized performance and compression.

2. **Use a More Complex Transformation**

   * Group by `city` and calculate the average age, then write the result to GCS.

3. **Parameterize the Input/Output Paths**

   * Use `sys.argv[]` or Spark configs to accept GCS paths dynamically at runtime.

4. **Schedule with Cloud Composer or Workflow**

   * Automate this ETL process with Cloud Composer or Dataproc Workflows for repeatability.

5. **Enable Logging and Monitoring**

   * Enable Stackdriver (Cloud Logging & Monitoring) to observe job-level metrics and logs.
