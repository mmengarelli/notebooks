# Databricks notebook source
sql("drop table if exists mikem.dummy_json")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType

schema = StructType([StructField("id", LongType())])
  
df = spark.readStream.schema(schema) \
.format("parquet") \
.option("path", "/mnt/mikem/data/dummy") \
.option("maxFilesPerTrigger", 1) \
.load()

# COMMAND ----------

df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json") \
  .table("mikem.dummy_json")

# COMMAND ----------

# df.writeStream \
#   .format("console") \
#   .start()

# COMMAND ----------

# import time
# time.sleep(100)

# for s in spark.streams.active:
#   s.stop()
