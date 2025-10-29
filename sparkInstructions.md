## Spark Containerized Installation

for most updated version of this and other examples: (https://github.com/PEPPythonData/Examples)

1. [install podman and podman desktop](https://podman.io/docs/installation)

2. In powershell or terminal with a command line of your choice use follow commands:
```
podman machine start
```

3. Then, also in powershell (or terminal of your choice):
```
podman network create my_network
```

4. In powershell again, navigate to the present working directory you are going to work in

5. Create a generic File and name it `Containerfile` in VSCode or Notepad++, ensure it does not have a .txt extension or any other extension, and paste
the following into the `Containerfile`:
```docker
FROM apache/spark

# Switch to root user to install Python
USER root

# Update and install Python3 and pip
RUN apt-get update && apt-get install -y python3 python3-pip vim nano 

# Install PySpark
RUN pip install pyspark

# Switch back to spark user (replace 'spark' with the appropriate user if it's different)
USER spark

# Set workdir
WORKDIR /opt/spark/work-dir

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3
```

6. With the powershell open to the current working directory location that contains the `Containerfile`, and already having run `podman machine start` type the following command in the terminal window:
```
podman build -t custom-spark-image .
```

7. once the previous steps completes, enter the following command in the same window:
```
podman run -d --name spark-master --network my_network -p 7077:7077 -p 8080:8080 custom-spark-image /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
```

8. Similarly, after above step enter the following command:
```
podman run -d --name spark-worker --network my_network -p 8081:8081 custom-spark-image /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
```

9. when you type the below command in the same terminal window, you should see the details of your containers running
```
podman ps
```

10. Now to access the terminal inside the container, in your terminal window, type the following command:
```
podman exec -it spark-master /bin/sh
```

10. Now you can run commands inside the container from your powershell or terminal of your choice, first let's create a basic pyspark program using our text editor vim. First let's change directories to where we want to create the file using the following command:
```bash
cd /opt/spark/work-dir
```

11. If you type `pwd` you should see `/opt/spark/work-dir`

12. Now create the file using the below command
```bash
touch sparkTest.py
```

13. Now open and edit the file using vim (or nano), you may want to research the basic vim commands but I will give you a few here. To open the file type the following:
```bash
vim sparkTest.py
```

14. do not press any characters yet, if you accidentally pressed something, first hit `ESC`, this will take you out of any mode you may be in, then type `:q!` to quit the editor without saving, this is an important command to remember.

15. step 14 was optional, if you have not pressed anything yet, now that you are in the vim editor, press the `Insert` key to enter insert mode

16. Now you can type and copy, paste etc, as you would expect with a basic text editor, but you may want to look up some extra commands to be able to work faster. At this point copy and paste the below into sparkTest.py:
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

# Test your Spark session
df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
df.show()

data = [("John", 28), ("Jane", 25)]
columns = ["Name", "Age"]


df2 = spark.createDataFrame(data, columns)
df2.show()
df3=df2.coalesce(1)

df3.write.format("csv").mode("overwrite").save("file:///opt/spark/work-dir/output")


# Get the SparkContext from the SparkSession
sc = spark.sparkContext

# Create an RDD from a collection (e.g., a list)
dataRDD = [1, 2, 3, 4, 5]
rdd = sc.parallelize(dataRDD)

# Perform an action to verify the RDD
print(rdd.collect())  # Output: [1, 2, 3, 4, 5]
```

17. Now type `ESC` to exit INSERT mode.

18. Then type `:wq` which will write your saved changes and then quit

19. You are ready to run your first spark program now. Just type in the terminal, with the python program in your current working directory:
```
python3 sparkTest.py
```

20. To see the csv you wrote to a file type the below command in the containers terminal:
```
cat /opt/spark/work-dir/output/*.csv
```

21. (Optional) Alternatively you could run using spark-submit:
```
spark-submit /opt/spark/work-dir/sparkTest.py
```

22. to exit the container's terminal in powershell type:
```
exit
```
23. to stop the container type:
```podman stop spark-master```
```podman stop spark-worker```

24. next time you want to run the same container just type in powershell:
```
podman start spark-master
```

25. Also remember to start the worker next time you want to run spark, just type in powershell:
```
podman start spark-worker
```

26. and to access the container again with an interactive shell type:
```
podman exec -it spark-master /bin/bash
```

Now you have successfully installed Spark on top of Hadoop, here you can run PySpark code on a Spark cluster.

Note: if you prefer Docker, you can use Docker instead, the only different is to replace the word `podman` with `docker` in all commands and instead of a file named `Containerfile` to build your image, you will create a file named `Dockerfile`