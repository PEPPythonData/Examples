# Assignment: Building a Simple Spark ETL Pipeline on AWS

## **Overview**

In this assignment, you will create a basic **ETL (Extract, Transform, Load)** pipeline using **Apache Spark on Amazon EMR**. The pipeline will:

* **Extract** raw CSV data from **Amazon S3**
* **Transform** it using Spark DataFrame operations
* **Load** the cleaned data back into a new S3 location

This assignment mimics real-world big data workflows and gives you experience managing a distributed data pipeline in AWS.

## **Instructions**

### Step 1: Set Up Your Environment

1. **Create an S3 bucket** (or use an existing one):

   * Example: `your-bucket-name`

2. **Upload raw CSV data** to a folder in the bucket:

   * Example file: `users.csv`

   ```csv
   id,name,email,age
   1,Alice,alice@example.com,30
   2,Bob,bob@example.com,25
   3,Charlie,charlie@example.com,35
   ```

3. **Create output folder** in S3:

   * Example: `s3://your-bucket-name/output/`

### Step 2: Write a Spark ETL Script (Python or Scala)

Example in **Python** (`etl_job.py`):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleETL").getOrCreate()

# Extract
df = spark.read.csv("s3://your-bucket-name/input/users.csv", header=True, inferSchema=True)

# Transform
filtered_df = df.filter(df.age > 28)

# Load
filtered_df.write.mode("overwrite").csv("s3://your-bucket-name/output/filtered_users")
```

Upload `etl_job.py` to a `scripts/` folder in your S3 bucket.

### Step 3: Launch an EMR Cluster

1. Go to **Amazon EMR > Create cluster**
2. Select:

   * **Applications**: Spark
   * **EMR release**: emr-6.x
   * **Instance types**: Use `m5.xlarge` or `t3.medium` (for low cost)
   * **Logging**: Enable and point to your S3 bucket
   * **Security**: Attach an IAM role with `AmazonS3FullAccess` and `AmazonEMRFullAccess`
3. Launch the cluster and wait for it to be in the **“Waiting”** state.

### Step 4: Run the ETL Job

1. SSH into the EMR **master node**:

   ```bash
   ssh -i your-key.pem hadoop@<master-node-public-dns>
   ```

2. Run the Spark ETL job using `spark-submit`:

   ```bash
   spark-submit s3://your-bucket-name/scripts/etl_job.py
   ```

3. Wait for the job to complete, and then check the output in your S3 bucket under `/output/filtered_users`.

### Step 5: Verify the Results

1. Open the output folder in S3.
2. Download and inspect the result CSV(s).
3. Confirm that only users with age > 28 are included.

## **Requirements**

* A working Spark ETL script that performs an extract-transform-load operation
* Raw data must be stored in S3 and processed by Spark
* The result must be written back to a different location in the same bucket
* The job must be executed successfully on an EMR cluster
* Output must be verified through S3

## Stretch Goals (Optional Enhancements)

1. **Add Data Cleansing Logic**

   * Remove rows with missing or malformed emails

2. **Write Output in Parquet Format**

   * Replace `.csv` with `.parquet` for better performance

3. **Parameterize the ETL Job**

   * Use `sys.argv[]` or environment variables to accept input/output paths dynamically

4. **Automate the Pipeline**

   * Use **AWS Step Functions**, **Lambda**, or **Airflow (MWAA)** to trigger the Spark job automatically

5. **Enable Cost Optimization**

   * Use **spot instances** for worker nodes
   * Auto-terminate the cluster after job completion
