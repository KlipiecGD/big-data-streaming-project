# Performed Optimizations

## Writing to GCS using NDJSON Format

**Description:** Instead of standard JSON arrays, the producer converts data into Newline-Delimited JSON (NDJSON). This allows Spark to read and process records line-by-line in parallel rather than loading an entire array into memory, which significantly reduces memory pressure and improves ingestion speed.

**Code Snippet:**

```python
# src/cloud_utils/save_to_gcs.py
# Convert list of records to newline-delimited JSON format for better streaming support
ndjson_data = '\n'.join(json.dumps(record) for record in data)

blob.upload_from_string(data=ndjson_data, content_type="application/json")
```
**References:**
- [Spark Streaming - DataStreamReader.json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.json.html)
- [What about NDJSON makes it better suited than standard JSON?](https://www.reddit.com/r/learnprogramming/comments/1i0sapp/what_about_ndjson_makes_it_better_suited_than/)
- [Faster Page Loads: How to Use NDJSON to Stream API Responses](https://www.bitovi.com/blog/faster-page-loads-how-to-use-ndjson-to-stream-api-responses)

## `spark.shuffle.partitions = 1`

**Description:** The default Spark configuration uses 200 shuffle partitions, which is overkill for the current volume of cryptocurrency data (250 records per minute). Setting this value to 1 minimizes the overhead of managing many small tasks and network shuffling, leading to faster micro-batch completion.

**Code Snippet:**

```python
# src/streaming/silver/processor.py and src/streaming/gold/processor.py
.config("spark.sql.shuffle.partitions", "1")
```
**References:**
- [Spark SQL - Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html#tuning-partitions)
- [Databricks Spark Jobs Optimization Techniques: Shuffle Partition Technique](https://fractal.ai/blog/databricks-spark-jobs-optimization-techniques-shuffle-partition-technique)

## Schemas Defined for DataFrames

**Description:** Explicitly defining `StructType` schemas for the Bronze and Silver layers prevents Spark from performing expensive schema inference. This reduces startup time and ensures the pipeline remains stable if the API response structure changes.

**Code Snippet:**

```python
# src/schemas/crypto_schema.py
CRYPTO_BRONZE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    # ... other fields
])

# Applying schema in src/streaming/silver/processor.py
raw_stream = (
    spark.readStream.schema(CRYPTO_BRONZE_SCHEMA)
    .json(bronze_path)
)
```
**References:**
- [Spark SQL - DataFrame.schema](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.schema.html)
- [Simplifying PySpark DataFrame Schema Creation](https://medium.com/@harshavardhan.achyuta/simplifying-pyspark-dataframe-schema-creation-9a013461b4d6)
- [PySpark Explained: The InferSchema Problem](https://towardsdatascience.com/pyspark-explained-the-inferschema-problem-12a08c989371/)

## Kryo Serialization

**Description:** Switching from default Java serialization to Kryo makes the serialization process significantly faster and the resulting data more compact. This reduces the time spent on "shuffling" data between Spark executors, which is especially beneficial for the windowed aggregations in the Gold layer.

**Code Snippet:**

```python
# src/streaming/silver/processor.py and src/streaming/gold/processor.py
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
.config("spark.kryo.registrationRequired", "false")
```

**References:**
- [Apache Spark - Kryo Serialization](https://spark.apache.org/docs/latest/tuning.html#data-serialization)
- [Java Serialization vs Kryo: A PySpark Performance Comparison](https://www.linkedin.com/pulse/java-serialization-vs-kryo-pyspark-performance-comparison-shaik-3agnf/)

## Trigger Intervals for Streaming Queries

**Description:** Both processing layers use a `1 minute` trigger interval. This interval is synchronized with the API producer's 60-second fetch cycle, ensuring that Spark only processes data when new files are available, which optimizes resource utilization and reduces cloud costs.

**Code Snippet:**

```python
# src/streaming/silver/processor.py 
query = (
    cleaned_stream.writeStream.format("parquet")
    .trigger(processingTime="1 minute") 
    .option("path", silver_path)
    # ...
)

# src/streaming/gold/processor.py
query_detailed = ( 
    transformed_df.writeStream.foreachBatch(write_main_data_to_bigquery)
    .trigger(processingTime="1 minute")
    # ...
)
query_rolling_avg = (
    rolling_avg_df.writeStream.foreachBatch(write_rolling_avg_to_bigquery)
    .trigger(processingTime="1 minute")
    # ...
)
```
**References:**
- [Databricks - Structured Streaming Triggers](https://docs.databricks.com/aws/en/structured-streaming/triggers.html)
- [Azure Databricks - Structured Streaming Triggers](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)
- [Trigger Modes in Apache Spark Structured Streaming](https://medium.com/@kiranvutukuri/trigger-modes-in-apache-spark-structured-streaming-part-6-91107a69de39)

## Partitioning by Date for saving Parquet Files

**Description:** Storing the Silver layer data in Parquet format partitioned by `date` enables "partition pruning". This allows downstream analytical queries (like those in BigQuery or Spark SQL) to skip irrelevant directories and only read data for specific days, drastically reducing I/O and query time.

**Code Snippet:**

```python
# src/streaming/silver/processor.py
query = (
    cleaned_stream.writeStream.format("parquet")
    .outputMode("append")
    .partitionBy("date")
    .start()
)
```

**References:**
- [Optimizing Apache Spark Performance with Partitioning](https://medium.com/@omkarspatil2611/optimizing-apache-spark-performance-with-partitionby-808f9728f033)
- [Apache Spark Partitioning and Bucketing](https://blog.dataengineerthings.org/apache-spark-partitioning-and-bucketing-1790586e8917)