# Spark Containerized Installation

## What you're building

In this activity, you'll set up a small **Apache Spark standalone cluster** using Podman:

- **1 Spark master**,  coordinates applications and assigns work.
- **1 Spark worker**,  provides compute resources and runs Spark executors.
- **1 PySpark application**,  submitted to the standalone cluster with `spark-submit`.

The application will use the **DataFrame API and RDD API**, and its CSV output will be written to a directory shared by both containers.

This activity does **not** use Hadoop or HDFS. Spark can run independently of Hadoop, and this exercise uses a shared local directory for simple file output.

> **Important:** This activity intentionally uses Spark's **standalone cluster manager**. The PySpark application is submitted to `spark://spark-master:7077`; it is not run with `local[*]`.

---

## Prerequisites

You need:

1. Podman and Podman Desktop installed.
2. A terminal such as:
   - PowerShell on Windows
   - Terminal on macOS/Linux
   - Git Bash on Windows (optional)
3. Basic familiarity with Python.

### Install Podman

Follow the official Podman installation instructions:

https://podman.io/docs/installation

### Start the Podman machine

On Windows or macOS, start the Podman virtual machine:

```bash
podman machine start
```

On Linux, skip this step.

---

# Part 1: Create the Spark Image

## 1. Create a working directory

Create a directory for this activity and move into it.

For example:

```bash
mkdir spark-standalone
cd spark-standalone
```

All commands in the remaining instructions should be run from this directory unless stated otherwise.

---

## 2. Create the shared output directory

Create a directory that will be mounted into **both** the master and worker containers:

```bash
mkdir spark-shared
```

This directory is important because the master and worker have separate container filesystems. A path such as `/opt/spark/work-dir/output` inside the master is **not automatically the same directory** inside the worker.

The shared directory gives both containers access to the same output location.

---

## 3. Create the Containerfile

Create a new, extension-less file named:

```text
Containerfile
```

Add the following:

```dockerfile
FROM apache/spark:3.5.9-python3

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/spark/work-dir
```

This uses the official Apache Spark 3.5.9 Python-enabled image. The image already contains Spark and Python support, so there is no need to install another copy of PySpark with `pip`.

The Spark version is pinned rather than using `latest`, making the exercise more reproducible.

---

## 4. Build the image

Run:

```bash
podman build -t custom-spark-image.
```

You should see the image build successfully.

Verify it:

```bash
podman images
```

Look for:

```text
custom-spark-image
```

---

# Part 2: Create the Spark Network

## 5. Create a dedicated Podman network

Create a network that the master and worker will share:

```bash
podman network create my_network
```

Containers attached to the same Podman network can communicate using their container names.

For example, the worker will connect to:

```text
spark://spark-master:7077
```

Here, `spark-master` is the container name and `7077` is the Spark standalone master's RPC port.

If you have already created this network and receive an "already exists" message, you can continue.

---

# Part 3: Start the Spark Master

## 6. Start the master container

Run:

```bash
podman run -d \
  --name spark-master \
  --network my_network \
  --mount type=bind,source="$(pwd)/spark-shared",target=/opt/spark/shared \
  -p 7077:7077 \
  -p 8080:8080 \
  custom-spark-image \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host spark-master \
  --port 7077 \
  --webui-port 8080
```

### Windows PowerShell

In PowerShell, use `${PWD}` instead of `$(pwd)`:

```powershell
podman run -d `
  --name spark-master `
  --network my_network `
  --mount type=bind,source="${PWD}/spark-shared",target=/opt/spark/shared `
  -p 7077:7077 `
  -p 8080:8080 `
  custom-spark-image `
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master `
  --host spark-master `
  --port 7077 `
  --webui-port 8080
```

The important ports are:

- `7077`,  Spark standalone master communication.
- `8080`,  Spark master web UI.

The shared directory is mounted at:

```text
/opt/spark/shared
```

inside the master container.

---

# Part 4: Start the Spark Worker

## 7. Start the worker container

Run:

```bash
podman run -d \
  --name spark-worker \
  --network my_network \
  --mount type=bind,source="$(pwd)/spark-shared",target=/opt/spark/shared \
  -p 8081:8081 \
  custom-spark-image \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  spark://spark-master:7077 \
  --host spark-worker \
  --webui-port 8081
```

### Windows PowerShell

```powershell
podman run -d `
  --name spark-worker `
  --network my_network `
  --mount type=bind,source="${PWD}/spark-shared",target=/opt/spark/shared `
  -p 8081:8081 `
  custom-spark-image `
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker `
  spark://spark-master:7077 `
  --host spark-worker `
  --webui-port 8081
```

The worker connects to:

```text
spark://spark-master:7077
```

because both containers are attached to `my_network`.

---

# Part 5: Verify the Cluster

## 8. Confirm both containers are running

Run:

```bash
podman ps
```

You should see both:

```text
spark-master
spark-worker
```

with a status such as:

```text
Up...
```

---

## 9. Check the Spark master UI

Open:

http://localhost:8080

The Spark master UI should show the worker registered with the cluster.

You should see information about the worker, including its status and available resources.

This confirms that:

```text
spark-master
     |
     |  spark://spark-master:7077
     |
spark-worker
```

is functioning as a Spark standalone cluster.

You can also open the worker UI:

http://localhost:8081

---

## 10. Check the container logs if the worker does not appear

If the worker does not appear in the master UI, check the logs:

```bash
podman logs spark-master
```

and:

```bash
podman logs spark-worker
```

The worker logs should indicate that it connected to the master.

---

# Part 6: Create the PySpark Application

## 11. Create `sparkTest.py`

Create a file named:

```text
sparkTest.py
```

Place it in the same directory as the `Containerfile`.

You can create it with:

```bash
touch sparkTest.py
```

---

## 12. Add the PySpark code

Open the file in an editor.

For example:

```bash
vim sparkTest.py
```

Or use another editor if you prefer.

Add:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
.appName("Standalone PySpark Test")
.getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Test the DataFrame API
df = spark.createDataFrame(
    [(1, "foo"), (2, "bar")],
    ["id", "value"]
)

df.show()

# Create another DataFrame
data = [
    ("John", 28),
    ("Jane", 25)
]

columns = ["Name", "Age"]

df2 = spark.createDataFrame(data, columns)

df2.show()

# Write a small CSV output.
# The shared directory is mounted in both containers.
# coalesce(1) makes the output easier to inspect for this small example.
df2.coalesce(1) \
.write \
.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save("file:///opt/spark/shared/output")

# Demonstrate the RDD API
sc = spark.sparkContext

data_rdd = [1, 2, 3, 4, 5]

rdd = sc.parallelize(data_rdd)

print(rdd.collect())

spark.stop()
```

The application will receive its master URL from `spark-submit`:

```text
spark://spark-master:7077
```

This is what makes the application run against the Spark standalone cluster rather than as a local Spark application.

---

# Part 7: Submit the Application to the Cluster

## 13. Open a shell in the master container

Run:

```bash
podman exec -it spark-master /bin/sh
```

The shell is now running inside the `spark-master` container.

---

## 14. Confirm the shared directory is available

Run:

```bash
ls -la /opt/spark/shared
```

You should see the directory contents.

The same host directory is mounted at `/opt/spark/shared` in both containers.

---

## 15. Confirm the application file is available

Because `sparkTest.py` is currently on the host, the master container needs access to it.

Exit the master container:

```bash
exit
```

Then start the master again with the application directory mounted.

First, stop and remove the existing master:

```bash
podman stop spark-master
podman rm spark-master
```

Then recreate it with both the shared output directory and the working directory mounted.

### Linux/macOS/Git Bash

```bash
podman run -d \
  --name spark-master \
  --network my_network \
  --mount type=bind,source="$(pwd)/spark-shared",target=/opt/spark/shared \
  --mount type=bind,source="$(pwd)",target=/opt/spark/work-dir \
  -p 7077:7077 \
  -p 8080:8080 \
  custom-spark-image \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host spark-master \
  --port 7077 \
  --webui-port 8080
```

### Windows PowerShell

```powershell
podman run -d `
  --name spark-master `
  --network my_network `
  --mount type=bind,source="${PWD}/spark-shared",target=/opt/spark/shared `
  --mount type=bind,source="${PWD}",target=/opt/spark/work-dir `
  -p 7077:7077 `
  -p 8080:8080 `
  custom-spark-image `
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master `
  --host spark-master `
  --port 7077 `
  --webui-port 8080
```

> **Why is the working directory mounted?**
>
> The PySpark file is stored on the host. Mounting the working directory makes `/opt/spark/work-dir/sparkTest.py` available inside the master container, where `spark-submit` will run it.

---

## 16. Submit the PySpark application

Open a shell inside the master:

```bash
podman exec -it spark-master /bin/sh
```

Then run:

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --total-executor-cores 1 \
  /opt/spark/work-dir/sparkTest.py
```

### What these options mean

| Option | Purpose |
|---|---|
| `--master spark://spark-master:7077` | Sends the application to the Spark standalone master |
| `--deploy-mode client` | Keeps the driver in the master container that runs `spark-submit` |
| `spark.driver.host=spark-master` | Tells the worker how to reach the driver |
| `spark.driver.bindAddress=0.0.0.0` | Allows the driver to accept connections from the worker |
| `--total-executor-cores 1` | Keeps this small exercise within the single worker's resources |
| `sparkTest.py` | The PySpark application being submitted |

The important difference from a local Spark application is:

```text
spark-submit
     |
     v
spark://spark-master:7077
     |
     v
Spark Master
     |
     v
Spark Worker
     |
     v
Executor
```

The worker now participates in executing the application.

---

# Part 8: Verify the Application Output

## 17. Verify the application ran successfully

The terminal should display the two DataFrames followed by:

```text
[1, 2, 3, 4, 5]
```

The application should then terminate normally.

---

## 18. Check the Spark master UI

While the application is running, open:

http://localhost:8080

The application should appear in the master's application list.

You can use the UI to see that the standalone master accepted the application and allocated resources to the worker.

---

## 19. Check the worker UI

Open:

http://localhost:8081

The worker UI provides information about the worker's resources and running/completed applications.

---

## 20. Inspect the CSV output from the host

Exit the master container:

```bash
exit
```

Then inspect the shared directory from the host:

```bash
ls spark-shared/output
```

You should see files similar to:

```text
part-00000-....csv
_SUCCESS
```

The exact generated filename is not fixed because Spark generates output part filenames.

To inspect the CSV on Linux/macOS/Git Bash:

```bash
cat spark-shared/output/*.csv
```

On PowerShell:

```powershell
Get-Content spark-shared/output/*.csv
```

You should see:

```text
Name,Age
John,28
Jane,25
```

The `_SUCCESS` file indicates that the Spark write completed successfully.

---

# Part 9: Understand What Happened

At this point, you have:

```text
                ┌──────────────────────┐
                │    Spark Master      │
                │  spark-master:7077   │
                └──────────┬───────────┘
                           │
                           │ assigns work
                           ▼
                ┌──────────────────────┐
                │    Spark Worker      │
                │  spark-worker:8081  │
                └──────────┬───────────┘
                           │
                           │ runs executor
                           ▼
                     PySpark tasks
```

The driver is running in the master container because this exercise uses **client deploy mode**.

The worker runs the executor that performs the distributed Spark tasks.

The master does not perform the executor's work simply because it is called the "master." Its primary role is cluster/resource coordination.

The application was submitted with:

```bash
--master spark://spark-master:7077
```

so it uses the standalone cluster rather than:

```text
local[*]
```

---

# Part 10: Stop the Cluster

## 21. Stop both containers

When you're finished:

```bash
podman stop spark-master
podman stop spark-worker
```

---

## 22. Start the cluster again later

The containers still exist, so you can start them again with:

```bash
podman start spark-master
podman start spark-worker
```

Then verify:

```bash
podman ps
```

Open:

http://localhost:8080

and confirm that the worker has registered again.

---

# Troubleshooting

## Worker does not appear in the master UI

Check:

```bash
podman logs spark-worker
```

Make sure the worker is connecting to:

```text
spark://spark-master:7077
```

Also verify that both containers are attached to:

```text
my_network
```

You can inspect the network with:

```bash
podman network inspect my_network
```

---

## `spark-submit` cannot connect to the master

Check that the master is running:

```bash
podman ps
```

Then check:

```bash
podman logs spark-master
```

The master should be listening on port `7077`.

---

## The application cannot connect back to the driver

Make sure the submission command includes:

```bash
--conf spark.driver.host=spark-master
```

and:

```bash
--conf spark.driver.bindAddress=0.0.0.0
```

These settings are important because the driver is running inside the master container while the executor is running in the worker container.

---

## CSV output cannot be found

Make sure the same host directory was mounted into **both** containers:

```text
host: spark-shared
       ↓
master: /opt/spark/shared
       ↓
worker: /opt/spark/shared
```

The Spark application writes to:

```text
file:///opt/spark/shared/output
```

Do not use:

```text
file:///opt/spark/work-dir/output
```

for this distributed example because the master and worker have separate container filesystems at that path.

---

## Permission denied when writing to `spark-shared`

On Linux, Podman may enforce host filesystem permissions or SELinux labeling.

If you are using a SELinux-enabled Linux system, add `:Z` to the bind mount, for example:

```bash
--mount type=bind,source="$(pwd)/spark-shared",target=/opt/spark/shared,Z
```

If your host filesystem prevents the container's unprivileged Spark user from writing to the directory, check the ownership and permissions of `spark-shared` on the host rather than changing the container to run as root.

Avoid using `chmod 777` as the default fix.

---

# Cleanup

If you want to completely remove the cluster:

```bash
podman stop spark-master spark-worker
podman rm spark-master spark-worker
podman network rm my_network
```

You can also remove the image if you no longer need it:

```bash
podman rmi custom-spark-image
```

The `spark-shared` directory on your host is not removed by these commands.

---

# Summary

You have now:

1. Installed and configured Podman.
2. Built a reproducible Python-enabled Spark image.
3. Created a dedicated Podman network.
4. Started a Spark standalone master.
5. Started a Spark standalone worker.
6. Verified that the worker registered with the master.
7. Created a PySpark application using DataFrames and RDDs.
8. Submitted the application to the standalone master with `spark-submit`.
9. Executed the application using the worker's resources.
10. Wrote output to a directory shared by the containers.
11. Inspected the generated CSV from the host.
12. Stopped and restarted the cluster.

The key architecture demonstrated by this activity is:

```text
Host
│
├── spark-master container
│   ├── Spark Master
│   ├── Spark Driver (client deploy mode)
│   └── /opt/spark/shared ─────┐
│                              │
└── spark-worker container     │
    ├── Spark Worker            │
    ├── Spark Executor          │
    └── /opt/spark/shared ─────┘
               │
               ▼
          Host/spark-shared
```

The important distinction is that this is a **real Spark standalone cluster exercise**, not a local-mode PySpark exercise. The application connects to `spark://spark-master:7077`, and the worker participates in execution.

### Verify that the Worker actually executed the application

After the job finishes, verify that the standalone cluster was actually used rather than only checking that the application completed successfully.

1. Open the Spark master UI at:

```text
http://localhost:8080
```

2. Under **Running Applications** or **Completed Applications**, locate your `Spark Standalone Test` application.

3. Confirm that the application shows an executor associated with `spark-worker`.

4. You can also inspect the worker UI at:

```text
http://localhost:8081
```

You should see the application listed there while it is running or in the worker's completed-application history.

> **This confirms:** The PySpark application was submitted to `spark://spark-master:7077` and executed through the standalone Spark cluster, rather than running only in local mode inside the master container.
