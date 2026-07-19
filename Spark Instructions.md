## Spark Containerized Installation

### What you're building

You're going to stand up a small **Spark standalone cluster**, one master node and one worker node, each running in its own container, and then submit a PySpark program to it. This is a simplified, local version of how Spark actually runs in production: a master that coordinates work, and one or more workers that execute it. Note that this setup does not involve Hadoop or HDFS. Spark can run entirely on its own, and here your PySpark script just reads and writes to the local container filesystem.

---

1. [Install Podman and Podman Desktop](https://podman.io/docs/installation).

2. Start the Podman VM (Windows/macOS only; skip this on Linux, where Podman doesn't need a VM):

```
podman machine start
```

3. Create a dedicated Podman network. Containers on the same network can reach each other by container name, which is how the worker will find and register itself with the master:

```
podman network create my_network
```

4. Navigate to the directory you want to work in.

5. Create a new, extension-less file named `Containerfile`, and paste in the following. This builds a custom image on top of the official Spark image, adding Python, pip, and PySpark so you can run Python-based Spark jobs (rather than only Scala/Java ones):

```docker
FROM apache/spark

# Switch to root user to install packages
USER root

# Install Python 3, pip, and the vim/nano text editors
RUN apt-get update && apt-get install -y python3 python3-pip vim nano

# Install PySpark
RUN pip install pyspark

# Switch back to the non-root "spark" user that the base image normally runs as
USER spark

# Set the working directory inside the container
WORKDIR /opt/spark/work-dir

# Tell Spark which Python interpreter to use for both the driver and executors
ENV PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

> **Note:** `FROM apache/spark` pulls whatever the `latest` tag currently points to. For reproducible builds, consider pinning an explicit version tag instead (e.g. `apache/spark:3.5.7`). Check the [apache/spark tags on Docker Hub](https://hub.docker.com/r/apache/spark/tags) for current options.

6. Build the image:

```
podman build -t custom-spark-image .
```

7. Start the Spark **master** container. This runs Spark's master process, which coordinates work across the cluster, and joins it to `my_network` so the worker can find it:

```
podman run -d --name spark-master --network my_network -p 7077:7077 -p 8080:8080 custom-spark-image /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
```

8. Start the Spark **worker** container. This is the process that actually executes the work the master assigns it. `spark://spark-master:7077` tells the worker where to find the master. That works because both containers are on `my_network` and can resolve each other by container name:

```
podman run -d --name spark-worker --network my_network -p 8081:8081 custom-spark-image /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

9. Confirm both containers are running:

```
podman ps
```

You should see `spark-master` and `spark-worker` both listed as "Up". You can also open `http://localhost:8080` in a browser to see the Spark master's web UI, which shows the worker registered underneath it.

10. Open an interactive shell inside the master container. This is where you'll write and run your PySpark script:

```
podman exec -it spark-master /bin/sh
```

11. Change into the working directory you configured in the `Containerfile`:

```bash
cd /opt/spark/work-dir
```

12. Confirm you're in the right place:

```
pwd
```

This should print `/opt/spark/work-dir`.

13. Create an empty file for your Spark program:

```bash
touch sparkTest.py
```

14. Open it in vim (or nano):

```bash
vim sparkTest.py
```

15. If you haven't typed anything yet, skip this step. Otherwise, press `ESC`, then type `:q!` and press Enter to quit without saving. This is useful any time you want to back out of vim without keeping changes.

16. Press `Insert` (or `i`) to enter INSERT mode.

17. Paste the following into `sparkTest.py`. This script starts a local Spark session, creates a couple of small DataFrames to confirm everything works, writes one of them out to CSV, and then demonstrates the older RDD API alongside the newer DataFrame API:

```python
#!/usr/bin/env python3

from pyspark.sql import SparkSession

import os

os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

spark = SparkSession.builder \
    .appName("Local PySpark") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Test your Spark session with a simple DataFrame
df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
df.show()

data = [("John", 28), ("Jane", 25)]
columns = ["Name", "Age"]

df2 = spark.createDataFrame(data, columns)
df2.show()

# coalesce(1) forces Spark to write a single output file instead of
# splitting it across multiple partitions/files, which is easier to
# inspect for a small example like this
df3 = df2.coalesce(1)
df3.write.format("csv").mode("overwrite").save("file:///opt/spark/work-dir/output")

# Get the SparkContext from the SparkSession, for working with the
# older, lower-level RDD API
sc = spark.sparkContext

# Create an RDD from a plain Python list
dataRDD = [1, 2, 3, 4, 5]
rdd = sc.parallelize(dataRDD)

# collect() pulls the RDD's data back to the driver so we can print it
print(rdd.collect())  # Output: [1, 2, 3, 4, 5]
```

18. Press `ESC` to leave INSERT mode.

19. Type `:wq` and press Enter to save and quit.

20. Run your first Spark program. Note that `.master("local[*]")` in the script means Spark will run this particular job using all available cores on the _local_ machine, rather than distributing it across the master/worker cluster you just built. This keeps the example simple and reliable to run:

```
python3 sparkTest.py
```

You should see two small tables printed (from `df.show()` and `df2.show()`), followed by `[1, 2, 3, 4, 5]`.

21. Look at the CSV file your script wrote out:

```
cat /opt/spark/work-dir/output/*.csv
```

22. (Optional) You can also launch the same script using `spark-submit`, Spark's standard job-submission tool. This is the command you'd typically use in a real deployment rather than calling `python3` directly:

```
spark-submit /opt/spark/work-dir/sparkTest.py
```

23. Exit the container's shell:

```
exit
```

24. Stop both containers when you're done:

```
podman stop spark-master
podman stop spark-worker
```

25. Next time you want to pick back up, start both containers again (no need to rebuild):

```
podman start spark-master
podman start spark-worker
```

26. And reconnect to the master with an interactive shell the same way as before:

```
podman exec -it spark-master /bin/bash
```

You've now stood up a small Spark cluster (one master, one worker) and run a PySpark program against it, using both the DataFrame API and the RDD API.

**Using Docker instead of Podman:** the steps are identical. Just replace `podman` with `docker` everywhere, and name your build file `Dockerfile` instead of `Containerfile`.

---

## Bonus: Persisting Data with a Volume Mount

Everything above works, but there's one limitation you may have noticed: the CSV output you wrote in step 21 only exists inside the `spark-master` container's filesystem. If you remove that container, the output goes with it, and you can't inspect it from your host machine without running `podman exec` first.

This optional section shows how to fix that by **bind-mounting** a folder from your host machine into the container, so the two share a folder in real time. It's not required for the core exercise above, but it's a useful pattern to understand, since "where did my data go?" is one of the most common points of confusion when people first start working with containers.

### Why this matters

Mounting a local folder means:

- Your data survives container restarts and rebuilds.
- You can inspect output directly from your host, without needing to `podman exec` into the container every time.

### Try it yourself

1. In the same directory as your `Containerfile`, create a `tmp` subfolder:

```
mkdir tmp
```

2. Instead of the `spark-master` command from step 7 above, start it with a `--mount` flag added. This maps `./tmp` on your host to `/tmp/mounted` inside the container:

```
podman run -d --name spark-master \
  --mount type=bind,source="$(pwd)/tmp",target=/tmp/mounted \
  --network my_network -p 7077:7077 -p 8080:8080 \
  custom-spark-image /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
```

> **Windows PowerShell note:** `$(pwd)` only works in a Unix-style shell (bash/zsh, or Git Bash on Windows). In plain PowerShell, use `${PWD}` instead:
>
> ```
> --mount type=bind,source="${PWD}/tmp",target=/tmp/mounted
> ```

3. Start `spark-worker` exactly as in step 8. It doesn't need the mount, since your PySpark script (running as the driver on `spark-master`) is the process doing the file reading and writing:

```
podman run -d --name spark-worker --network my_network -p 8081:8081 custom-spark-image /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

4. Try modifying `sparkTest.py` so it writes its CSV output to `/tmp/mounted/output` instead of `/opt/spark/work-dir/output`, then re-run it. Afterwards, check the `tmp` folder on your own host machine. You should see the output show up there directly, without needing to enter the container at all.
