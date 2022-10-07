# Databricks notebook source
schema = spark.read.json("/mnt/training/retail-org/solutions/sales_stream/12-05.json").schema

df = spark.readStream.schema(schema) \
  .format("json") \
  .option("path", "/mnt/training/retail-org/solutions/sales_stream") \
  .option("maxFilesPerTrigger", 1) \
  .load()

# COMMAND ----------

df.writeStream \
  .format("console") \
  .trigger(processingTime='300 seconds') \
  .queryName("stream_1") \
  .trigger(once=True) \
  .option("checkpointLocation", "dbfs:/mnt/mikem/checkpoints/dummy2") \
  .start()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/mikem/checkpoints/dummy1

# COMMAND ----------

for s in spark.streams.active:
  print(s.name)
  #s.stop()
