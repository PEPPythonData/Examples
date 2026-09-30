## Hive Containerized Installation

### What you're building

You're going to run Apache Hive inside a container, using Podman (a Docker-compatible container engine). By the end of this walkthrough you'll have a running Hive server that you can connect to with `beeline` (Hive's command-line client) and use to create tables and run HiveQL queries.

Hive itself needs Hadoop underneath it to store and process data (that's what "data warehousing on top of Hadoop" means). The `apache/hive` container image already bundles a compatible Hadoop distribution for you, so you don't need to install Hadoop separately for this exercise.

---

1. [Install Podman and Podman Desktop](https://podman.io/docs/installation). Podman Desktop gives you a GUI for managing containers if you prefer that over the command line.

2. Open PowerShell (or the terminal of your choice) and start the Podman virtual machine. This step is only needed on Windows and macOS, since Podman runs containers inside a small Linux VM on those platforms. On Linux, Podman talks to the OS directly, so you can skip this command:

```
podman machine start
```

3. In the same terminal, navigate (`cd`) to the directory you want to work in. This is where you'll create your container's build instructions.

4. Create a new, extension-less file named `Containerfile` (in VS Code, Notepad++, or any text editor). This file tells Podman how to build a custom image on top of the official Hive image, adding Python and a couple of text editors so you can write and run scripts inside the container later. Paste in the following:

```docker
FROM apache/hive:4.2.1

# Switch to the root user so we have permission to install packages
USER root

# Install Python 3, pip, and the vim/nano text editors
RUN microdnf install -y python3 python3-pip vim nano

# Switch back to the non-root "hive" user that the base image normally runs as
USER hive
```

> **Note:** The current `apache/hive` images are built on Red Hat UBI 9, which uses `microdnf` as its package manager. Older tags such as 4.0.1 were Ubuntu-based and used `apt-get`, so if you pin an older tag, swap that line back. This guide was tested with `4.2.1`. Check the [apache/hive tags on Docker Hub](https://hub.docker.com/r/apache/hive/tags) for newer releases.

5. With your terminal still open in the folder containing the `Containerfile`, and having already run `podman machine start`, build your custom image. This reads the `Containerfile` and produces a reusable image named `custom-hive-image`:

```
podman build -t custom-hive-image .
```

6. **Windows and macOS only: turn off cgroups in the Podman VM.** By default the Podman VM runs in rootless mode, and on some setups it doesn't have the `pids` cgroup controller available. Without this step, starting the container can fail with `crun: controller 'pids' is not available ... OCI runtime error`. Linux users can skip to step 7.

Make sure the machine is running (step 2), then connect to it from PowerShell:

```
podman machine ssh
```

Your prompt will change to something like `[user@...]`. You're now inside the VM. Run this there:

```
mkdir -p ~/.config/containers && echo -e '[containers]\ncgroups = "disabled"' > ~/.config/containers/containers.conf
exit
```

Back in PowerShell, restart the machine so it picks up the new setting:

```
podman machine stop
podman machine start
```

With cgroups disabled, flags like `--memory` and `--cpus` are no longer enforced, which doesn't matter for this exercise. The setting lives inside the VM, so if you ever delete and recreate the machine with `podman machine rm`, you'll need to repeat this step.

7. Once the build finishes, start a container from that image. This launches Hive with its built-in Derby database as the metastore. That's fine for learning and testing, but not for production use, since Derby doesn't handle concurrent access well. The command also exposes two ports: 10000 for JDBC clients like beeline, and 10002 for the web UI:

```
podman run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 custom-hive-image
```

If you get a "name already in use" error, a container from an earlier failed attempt is still around. Remove it with `podman rm -f hive4` and run the command again.

8. Confirm the container is up and running:

```
podman ps
```

You should see `hive4` listed with a status of "Up".

9. Give HiveServer2 a minute or so to finish starting up in the background before connecting. If you skip this, beeline may fail to connect on the first try. (`podman logs hive4` shows what it's doing if you want to check.) Then open an interactive shell inside the running container:

```
podman exec -it hive4 /bin/sh
```

10. From inside the container, connect to HiveServer2 using beeline, Hive's command-line SQL client (similar in spirit to `psql` for Postgres or `mysql` for MySQL):

```
beeline -u "jdbc:hive2://localhost:10000" -n hive
```

11. Once you're in the beeline prompt, create a table. This defines a table named `hive_example` with two columns: a string column `a` and an integer column `b`:

```sql
CREATE TABLE hive_example(a string, b int);
```

12. Confirm your table was created:

```sql
SHOW TABLES;
```

13. You should see `hive_example` in the output. Now inspect it in more detail. This command shows metadata like column types, and also the HDFS/warehouse location where Hive is physically storing the table's data:

```sql
DESCRIBE FORMATTED hive_example;
```

14. Insert a few rows and run some basic aggregate queries against them. Note that this uses the same table name, `hive_example`, that you created in step 11. A common mistake is inserting into a differently-named table that was never created, which will fail:

```sql
INSERT INTO hive_example VALUES ('x', 1), ('x', 2), ('y', 3);
SELECT COUNT(DISTINCT a) FROM hive_example;
SELECT SUM(b) FROM hive_example;
```

The first query should return `2` (two distinct values of `a`: `x` and `y`), and the second should return `6` (1 + 2 + 3).

15. To leave beeline, use its built-in quit command. Don't use `ctrl+c`, which just kills your terminal session ungracefully:

```
!quit
```

Then, to leave the container's shell entirely:

```
exit
```

16. When you're done for the day, stop the container to free up resources:

```
podman stop hive4
```

17. Next time you want to pick back up, you don't need to rebuild anything. Just start the existing container again:

```
podman start hive4
```

18. And reconnect with an interactive shell the same way as before:

```
podman exec -it hive4 /bin/sh
```

You've now installed Hive, backed by its bundled Hadoop, in a container, and can connect to it with beeline to create and query tables.

**Using Docker instead of Podman:** the steps are identical. Just replace `podman` with `docker` everywhere, and name your build file `Dockerfile` instead of `Containerfile`. Step 6 (the cgroup fix) is Podman-specific, so skip it with Docker.
