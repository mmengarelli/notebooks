# Databricks notebook source
spark.conf.set("spark.sql.sources.commitProtocolClass","org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
spark.conf.set("parquet.enable.summary-metadata", "false")

# COMMAND ----------

# MAGIC %fs rm -r /mnt/mikem/data/dummy

# COMMAND ----------

import time

for x in range(100):
  df = spark.range(10000).repartition(100)
  df.write.mode('append').parquet("/mnt/mikem/data/dummy")
  time.sleep(10)
