# Databricks notebook source
dbutils.fs.rm("/mnt/mikem/out/autoloader/lending_club/", True)
dbutils.fs.rm("/mnt/mikem/checkpoints/autoloader1", True)

# COMMAND ----------

schema = spark.read.option("header", True).csv("dbfs:/databricks-datasets/lending-club-loan-stats/LoanStats_2018Q2.csv").schema

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .schema(schema) \
  .load("/mnt/mikem/data/lending_club_csv/")

# COMMAND ----------

# MAGIC %fs ls /mnt/mikem/data/lending_club_csv/

# COMMAND ----------

df.writeStream.format("delta") \
  .option("checkpointLocation", "/mnt/mikem/checkpoints/autoloader1") \
  .trigger(processingTime='10 seconds') \
  .start("/mnt/mikem/out/autoloader/lending_club/")

# COMMAND ----------

import time
time.sleep(10)

# COMMAND ----------

# MAGIC %sql select count(1) from delta.`/mnt/mikem/out/autoloader/lending_club/`
