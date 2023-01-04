# Databricks notebook source
path = "/mnt/mikem/data/autoloaderfullpath"
schema_location = "/tmp/autoloaderfullpath/schema"
checkpoint_location = "/mnt/mikem/chkpts/autoloaderfullpath"
table_name = "mikemautoloaders.autoloaderfullpath"

dbutils.fs.rm(path, True)
dbutils.fs.rm(schema_location, True)
dbutils.fs.rm(checkpoint_location, True)

sql(f"drop table if exists {table_name}")

# COMMAND ----------

from pyspark.sql.functions import input_file_name

stream_df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.schemaLocation", schema_location) \
  .load(path) \
  .drop("_rescued_data") \
  .withColumn("filePath", input_file_name())

stream_df.writeStream \
  .option('checkpointLocation', checkpoint_location) \
  .option("maxFilesPerTrigger", 1) \
  .outputMode("append") \
  .toTable("mikem_dummy_autoloader")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from mikemautoloaders.autoloaderfullpath

# COMMAND ----------

# DBTITLE 1,Stop all Streams
[s.stop() for s in spark.streams.active]