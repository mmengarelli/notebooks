# Databricks notebook source
# MAGIC %md # Structured Streaming using the Python DataFrames API
# MAGIC 
# MAGIC Apache Spark includes a high-level stream processing API, [Structured Streaming](http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). In this notebook we take a quick look at how to use the DataFrame API to build Structured Streaming applications. We want to compute real-time metrics like running counts and windowed counts on a stream of timestamped actions (e.g. Open, Close, etc).
# MAGIC 
# MAGIC To run this notebook, import it and attach it to a Spark cluster.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = "/databricks-datasets/structured-streaming/events/"

jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

# COMMAND ----------

df1 = spark.read.schema(jsonSchema).json(inputPath)
df2 = spark.read.schema(jsonSchema).json(inputPath)
df3 = spark.read.schema(jsonSchema).json(inputPath)
df4 = spark.read.schema(jsonSchema).json(inputPath)
df5 = spark.read.schema(jsonSchema).json(inputPath)
df6 = spark.read.schema(jsonSchema).json(inputPath)
df7 = spark.read.schema(jsonSchema).json(inputPath)
df8 = spark.read.schema(jsonSchema).json(inputPath)

df9 = (
  df1.unionAll(df2)
.unionAll(df3)
.unionAll(df4)
.unionAll(df5)
.unionAll(df6)
.unionAll(df7)
.unionAll(df8)
)

# COMMAND ----------

df9.count()

# COMMAND ----------

# MAGIC %fs rm -r /mnt/mikem/data/etl-from-json

# COMMAND ----------

df9.repartition(800000).write.json("/mnt/mikem/data/etl-from-json")

# COMMAND ----------

events = (
  spark
    .readStream                       
    .schema(jsonSchema)               
    .option("maxFilesPerTrigger", 1)  
    .json(inputPath)
)

# COMMAND ----------

query = (
  events
    .writeStream
    .format("delta")          
    .outputMode("append")
    .option("checkpointLocation", "/mnt/mikem/checkpoints/etl-from-json")
    .start("/mnt/mikem/delta/etl-from-json")
)

# COMMAND ----------

# MAGIC %fs head dbfs:/mnt/mikem/checkpoints/etl-from-json/offsets/0

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from delta.`/mnt/mikem/delta/etl-from-json`
