## Hadoop MapReduce Executed in a Container

### What you're building

This walkthrough writes the "hello world" of big data: a word count, using the classic MapReduce pattern. A **mapper** turns each line of text into `(word, 1)` pairs, and a **reducer** sums those pairs up per word. You'll write both in plain Python and run them two ways. First you'll pipe them together yourself on the command line, so you can see exactly what each stage does. Then you'll use Hadoop's real streaming API, which is how MapReduce jobs actually get submitted to a cluster.

You'll do this inside the same Hive container from `hiveInstructions.md`, since it already bundles a working Hadoop installation, so you don't need a separate Hadoop setup.

---

### Prerequisites

1. [Install Podman and Podman Desktop](https://podman.io/docs/installation) if you haven't already.

2. Complete the Hive Containerized Installation in `hiveInstructions.md` first. You should end up with a container named `hive4` that you can start and stop.

---

### Starting the container

3. Start the Podman VM (Windows/macOS only; skip on Linux):

```
podman machine start
```

4. Start your existing `hive4` container:

```
podman start hive4
```

5. Open an interactive shell inside it:

```
podman exec -it hive4 /bin/bash
```

### Writing the mapper and reducer scripts

6. Change into the `/tmp` directory. You need write permission to create and edit files, and `/tmp` is writable by any user inside the container:

```
cd /tmp
```

7. Create an empty file for your mapper:

```
touch mapper.py
```

8. Create an empty file for your reducer:

```
touch reducer.py
```

9. Confirm both files exist:

```
ls
```

10. Open `mapper.py` in vim (or nano, if you're more comfortable there) to start editing it:

```bash
vim mapper.py
```

11. If you haven't touched anything yet, skip this step. Otherwise, press `ESC` to leave whatever mode you're in, then type `:q!` and press Enter to quit without saving. This is worth remembering any time you want to bail out of vim without keeping changes.

12. Press the `Insert` key (or `i`) to enter INSERT mode, which lets you type and edit text normally.

13. Paste the following into `mapper.py`. This script reads text line by line from standard input, splits each line into words, and prints each word out paired with the number `1`. That's the raw material the reducer will later sum up:

```python
#!/usr/bin/env python3
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print('%s\t%s' % (word, 1))
```

14. Press `ESC` to leave INSERT mode.

15. Type `:wq` and press Enter to write (save) your changes and quit vim.

16. Now do the same for `reducer.py`. Open it:

```bash
vim reducer.py
```

Enter INSERT mode, and paste the following. This script relies on the fact that Hadoop sorts the mapper's output by key (word) before handing it to the reducer, so all occurrences of the same word arrive next to each other. It walks through that sorted input, accumulating a running count for the current word, and prints the total each time the word changes:

```python
#!/usr/bin/env python3
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# do not forget to output the last word!
if current_word == word:
    print('%s\t%s' % (current_word, current_count))
```

Save and quit the same way: `ESC`, then `:wq`.

### Making the scripts runnable

17. Both scripts need execute permission before Hadoop (or you, directly) can run them. `chmod 777` grants read/write/execute to everyone, which is fine for this local learning exercise but is broader than you'd want on a shared or production system. A more scoped-down `chmod 755` would also work here:

```
chmod 777 mapper.py
chmod 777 reducer.py
```

### Testing the mapper and reducer yourself, without Hadoop

Before handing this off to Hadoop, it's worth running the pipeline by hand so you can see exactly what's happening at each stage. This is also a handy way to debug a mapper/reducer pair before submitting it as a real job.

18. Pipe a string of words straight into your mapper. You should see each word printed out on its own line, paired with a `1`:

```
echo "foo foo quux labs foo bar quux" | ./mapper.py
```

19. Now chain the mapper's output through `sort` (to group identical words together, mimicking what Hadoop does automatically) and into the reducer. You should get back a word count: `foo` appears 3 times, `quux` 2 times, and `labs`/`bar` once each:

```
echo "foo foo quux labs foo bar quux" | ./mapper.py | sort -k1,1 | ./reducer.py
```

20. Let's do the same thing, but reading from a file instead of typing text directly into the pipe. First, create the file:

```
cat > MR.txt
```

21. Your terminal is now waiting for input. Paste in the same sample text, press Enter, then press `ctrl+d` to signal end-of-input and finish writing the file:

```
foo foo quux labs foo bar quux
```

22. Confirm the file was written correctly:

```
cat MR.txt
```

23. Run the same mapper/reducer pipeline, this time reading from the file:

```
cat MR.txt | ./mapper.py | sort -k1,1 | ./reducer.py
```

### Running it for real, through Hadoop

24. Now use Hadoop's **streaming API**, a built-in utility that lets you write MapReduce jobs in any language (Python here) that can read from stdin and write to stdout, rather than requiring Java. This submits an actual MapReduce job to Hadoop, using your scripts as the mapper and reducer, with `MR.txt` as input:

```
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-file /tmp/mapper.py    -mapper /tmp/mapper.py \
-file /tmp/reducer.py   -reducer /tmp/reducer.py \
-input /tmp/MR.txt  -output /tmp/output
```

> **Note:** the `hadoop-streaming-3.3.6.jar` filename matches the Hadoop version (3.3.6) bundled with `apache/hive:4.0.1`. If you're using a different Hive/Hadoop version, run `ls /opt/hadoop/share/hadoop/tools/lib/` inside the container to find the exact jar filename to use.

> **Important:** Hadoop refuses to run a job if the output directory already exists (it won't silently overwrite results). If you re-run this command, first delete the old output with `hdfs dfs -rm -r /tmp/output` or `rm -rf /tmp/output`, depending on whether Hadoop is writing to HDFS or the local filesystem in your setup.

25. Once the job finishes, print the results Hadoop wrote out. You should see the same word counts as your manual run in step 19/23:

```
cat /tmp/output/*
```

You've now run the same word-count logic two ways: manually piped together so you could see each stage, and as a real Hadoop MapReduce job using the streaming API, which is the same mechanism used to run non-Java MapReduce jobs on a production cluster.
