# Structured Streaming Assignment

## Overview

This assignment introduces you to PySpark’s Structured Streaming API. You will simulate streaming JSON data from a folder, apply filtering transformations, and output results to the console. This helps reinforce the basics of setting up and working with streaming pipelines in PySpark.

---

## Instructions

### Step 1: Create Input Folder and Starter File

1. In your working directory, create a folder named `temp_input/`.
2. Inside `temp_input/`, create a file named `batch1.json` with the following content (one JSON object per line):

```json
{"device_id": "sensor_01", "timestamp": "2025-05-28T08:45:00", "temperature": 72.5}
{"device_id": "sensor_02", "timestamp": "2025-05-28T08:46:00", "temperature": 69.8}
{"device_id": "sensor_03", "timestamp": "2025-05-28T08:47:00", "temperature": 74.1}
```

You will use this file as the initial stream input.

---

### Step 2: Build the Streaming Pipeline

1. Set up a PySpark `SparkSession` with streaming support.
2. Define a schema using `StructType` to match the JSON structure.
3. Create a streaming DataFrame using `.readStream` from the `temp_input/` folder.
4. Apply the following transformations:

   * Select `device_id` and `temperature` columns.
   * Filter to only include rows where `temperature > 70`.

---

### Step 3: Write the Stream to the Console

1. Use `.writeStream` with the following options:

   * Format: `console`
   * Output mode: `append`
2. Start the stream and monitor the output.

To simulate additional streaming data, add a second file named `batch2.json` to the `temp_input/` folder after a few seconds:

```json
{"device_id": "sensor_04", "timestamp": "2025-05-28T08:48:00", "temperature": 68.3}
{"device_id": "sensor_01", "timestamp": "2025-05-28T08:49:00", "temperature": 75.2}
```

---

### Requirements

* Stream JSON data from a directory using `.readStream`.
* Use a predefined schema to read the input data.
* Apply both filtering and selection transformations.
* Output the filtered results to the console using `.writeStream`.
* Simulate a new micro-batch by manually adding a second file.
