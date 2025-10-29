## Hive Containerized Installation

for most updated version of this and other examples: (https://github.com/PEPPythonData/Examples)

1. [install podman and podman desktop](https://podman.io/docs/installation)

2. In powershell or terminal with a command line of your choice use follow commands:
```
podman machine start
```
3. In powershell again, navigate to the present working directory you are going to work in

4. Create a generic File and name it `Containerfile` in VSCode or Notepad++, ensure it does not have a .txt extension or any other extension, and paste
the following into the `Containerfile`:
```docker
FROM apache/hive:4.0.0

# Switch to root user to install Python
USER root

# Update and install Python3 and pip, also text editors vim and nano
RUN apt-get update && apt-get install -y python3 python3-pip vim nano 

# Switch back to hive user (replace 'hive' with the appropriate user if it's different)
USER hive
```

5. With the powershell open to the current working directory location that contains the `Containerfile`, and already having run `podman machine start` type the following command in the terminal window:
```
podman build -t custom-hive-image .
```

6. one the previous steps complets, enter the following command in the same window:
```
podman run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 custom-hive-image
```

7. when you type the below command in the same terminal window, you should see the details of your container running
```
podman ps
```

8. Now to access the terminal inside the container, in your terminal window, type the following command:
```
podman exec -it hive4 /bin/sh
```

9. Now you can run commands inside the container from your powershell or terminal of your choice, first let's run some basic hive commands using the beeline:
```
beeline -u "jdbc:hive2://localhost:10000" -n hive
```

10. In the beeline in your terminal create a new table with the following command:
```
CREATE TABLE hive_example(a string, b int);
```

11. still using beeline:
```
SHOW TABLES;
```

12. after the above command to show tables, you should see the name of your table displayed, then run the following beeline command to see more details about your table
such as the location of it's data:
```
DESCRIBE FORMATTED hive_example;  
```

13. Now insert some data and then run a couple queries in the beeline:
```
INSERT INTO hive_example2 values('x', 1), ('x', 2),('y',3);
SELECT COUNT(distinct a) FROM hive_example2;
SELECT SUM(b) FROM hive_example2;
```

14. to exit the beeline type `ctrl + c`, and then to exit the container type `exit`


15. to stop the container type:
```podman stop hive4```

16. next time you want to run the same container just type in powershell:
```
podman start hive4
```

17. and to access the container again with an interactive shell type:
```
podman exec -it hive4 /bin/bash
```

Now you have successfully installed Hive on top of Hadoop and can access your hive using the beeline, here you can create and access any hive tables you need.

Note: if you prefer Docker, you can use Docker instead, the only different is to replace the word `podman` with `docker` in all commands and instead of a file named `Containerfile` to build your image, you will create a file named `Dockerfile`