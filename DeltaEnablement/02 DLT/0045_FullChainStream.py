# Databricks notebook source
# MAGIC %md 
# MAGIC Live table reads<br>
# MAGIC Use `dlt.read("table")` or `spark.table("LIVE.table")`to perform a complete read from a dataset defined in the same pipeline. 
# MAGIC 
# MAGIC Streaming Read<br>
# MAGIC Use `dlt.read_stream("table")` to perform a streaming read from a dataset defined in the same pipeline.
# MAGIC 
# MAGIC Stream -> LIVE -> LIVE

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.table

# def evts_raw():
#   return spark.read.json("/databricks-datasets/structured-streaming/events/")


# def evts_raw():
#   return spark.readStream.format("cloudFiles") \
#     .option("cloudFiles.format", "json") \
#     .load("/databricks-datasets/structured-streaming/events/")

# COMMAND ----------

from pyspark.sql.functions import col 

def square(i: int) -> int:
  return i * i

spark.udf.register("makeItSquared", square)

@dlt.table
def evts_silver():
  return dlt.read("evts_raw") \
    .withColumn("time2", square(col("time")))

# COMMAND ----------

@dlt.table
def evts_gold():
  return dlt.read("evts_silver")
