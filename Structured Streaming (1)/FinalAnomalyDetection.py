# Databricks notebook source
# MAGIC %md ##### Read stream

# COMMAND ----------

# hack to get schema
schema = table("mikem.assets").schema

# COMMAND ----------

# mock stream
streaming = spark.readStream \
  .schema(schema) \
  .option("maxFilesPerTrigger", 1) \
  .parquet("/mnt/mikem/tmp/assets2")

# COMMAND ----------

streaming \
  .selectExpr("asset_id", "inference", "timestamp") \
  .writeStream \
  .trigger(processingTime='10 seconds') \
  .queryName("events_per_window") \
  .format("memory") \
  .outputMode("update") \
  .start()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *   
from pyspark.sql.functions import * 

df = table("events_per_window")

df2 = df.withColumn('InfLag', lag('Inference', 1).over(Window.partitionBy('Asset_ID').orderBy('Timestamp')))\
  .withColumn('Grouping', sum((col('Inference')!=col('InfLag')).cast('int')).over(Window.partitionBy('Asset_ID').orderBy('Timestamp')))\
  .withColumn('StreakStart', min('Timestamp').over(Window.partitionBy('Asset_ID', 'Grouping')))\
  .withColumn('InferenceStreak', col('Timestamp').cast('long') - col('StreakStart').cast('long'))\
  .withColumn('AnomalyRatePast6H', sum((col('Inference')==0).cast('int'))\
  .over(Window.partitionBy('Asset_ID').orderBy(unix_timestamp('Timestamp')).rangeBetween(-21599, 0)) / lit(24.0))\
  .withColumn('AnomalyRatePast24H', sum((col('Inference')==0).cast('int'))\
  .over(Window.partitionBy('Asset_ID').orderBy(unix_timestamp('Timestamp')).rangeBetween(-86399, 0)) / lit(96.0))

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md write as delta and use delta streaming source to get real time data

# COMMAND ----------

df2.write.format("delta").mode("append").save("/mnt/delta/anomalies")

# COMMAND ----------

df3 = spark.readStream.format("delta").load("/mnt/delta/anomalies")
display(df3)
