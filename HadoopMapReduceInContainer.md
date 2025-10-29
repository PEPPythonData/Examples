## Hadoop Map Reduce executed in Container

for most updated version of this and other examples: (https://github.com/PEPPythonData/Examples)

1. In order to run this example you must first [install podman and podman desktop](https://podman.io/docs/installation) and
have successfully completed the Hive Containerized Installation form the hiveInstructions.md 

2. Assuming you have competed the Hive Containerize Installation and a podman container exists named hive4, do the following in powershell (or the terminal you use for podman)
```
podman machine start
```
3. In the same powershell, start your hive container:
```
podman start hive4
```

4. Then to access your hive container with an interactive shell:
```
podman exec -it hive4 /bin/bash
```

5. change directories in your container to the /tmp folder using the following command:
```
cd /tmp
```

6. Now that you have changed directories to /tmp where you have full file permissions, create a new file named mapper.py using following command:
```
touch mapper.py
```
7. Also create a file named reducer.py
```
touch reducer.py
```
8. Verify both files exist in the present directory using following command:
```
ls
```

9. Now open and edit the file using vim (or nano), you may want to research the basic vim commands but I will give you a few here. To open the file type the following:
```bash
vim mapper.py
```

10. do not press any characters yet, if you accidentally pressed something, first hit `ESC`, this will take you out of any mode you may be in, then type `:q!` to quit the editor without saving, this is an important command to remember.

11. step 10 was optional, if you have not pressed anything yet, now that you are in the vim editor, press the `Insert` key to enter insert mode

12. Now you can type and copy, paste etc, as you would expect with a basic text editor, but you may want to look up some extra commands to be able to work faster. At this point copy and paste the below into sparkTest.py:
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
        print ('%s\t%s' % (word, 1))
```

13. Now type `ESC` to exit INSERT mode.

14. Then type `:wq` which will write your saved changes and then quit

15. Repeat these steps for reducer.py using vim to copy and paste the following into reducer.py:
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
            print ('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print ('%s\t%s' % (current_word, current_count))
```
16. we need to change the file permission's so we can run the two files, type the following in the interactive shell with reducer.py and mapper.py in the current directory:
```
chmod 777 mapper.py
```
17. Then do the same for reducer.py:
```
chmod 777 reducer.py
```

18. Now type the following command, piping the respective string into your mapper function:
```
echo "foo foo quux labs foo bar quux" | ./mapper.py
```

19. Then use the reducer to reduce the output from the mapper using the following command, which should return a word count:
```
echo "foo foo quux labs foo bar quux" | ./mapper.py | sort -k1,1 | ./reducer.py
```
20. Let's try it with a file as input this time, but first let's create the file by typing the following:
```
cat > MR.txt
```
21. There should still be a cursor waiting for input in the interactive terminal, paste the following and then press enter and then `ctrl+d` to end the command:
```
foo foo quux labs foo bar quux
```
22. If you type `cat MR.txt` you should have the content of the new file output in the interactive terminal

23. Now use your map and reduce functions to pipe the file as input and output a word count:
```
cat MR.txt | ./mapper.py | sort -k1,1 | ./reducer.py
```

24. Now that you have written your own map reduce functions to perform a word count, let's use hadoop's built-in utilities to provide a "streaming" API for running mappers and reducers that communicate with Hadoop via standard input (stdin) and standard output. Paste the following into the terminal:
```
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
-file /tmp/mapper.py    -mapper /tmp/mapper.py \
-file /tmp/reducer.py   -reducer /tmp/reducer.py \
-input /tmp/MR.txt  -output /tmp/output
```
(Note: make sure output directory is not created yet, if it is delete it before running above)

25. Now to see the output from hadoop's built-in mapreduce utilities and your custom map, reduce functionality, type the following in the terminal to print the wordcount:
```
cat /tmp/output/*
```