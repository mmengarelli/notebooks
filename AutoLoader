# Databricks notebook source
# DBTITLE 1,Setup
source_path = dbutils.widgets.get("source")
destination_table = dbutils.widgets.get("destination")
mode = dbutils.widgets.get("mode")

database_name = "mikem_autoloaders"
schema_location = f"/tmp/AutoLoader/{destination_table}/schema"
checkpoint_location = f"/mnt/mikem/chkpts/AutoLoader/{destination_table}"

print(f"Path: {source_path}")
print(f"Database: {database_name}")
print(f"Table: {destination_table}")
print(f"Mode: {mode}")
print(f"Schema Location: {schema_location}")
print(f"Checkpoint: {checkpoint_location}")

# Append or overwrite table with each micro-batch
def write_df(df, epoch_id):
  df.write.mode(mode).saveAsTable(f"{database_name}.{destination_table}")

sql(f"use {database_name}")

# COMMAND ----------

from pyspark.sql.functions import input_file_name

stream_df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "parquet") \
  .option("cloudFiles.schemaLocation", schema_location) \
  .option("cloudFiles.maxFilesPerTrigger", 1) \
  .load(source_path) \
  .drop("_rescued_data") \
  .withColumn("filePath", input_file_name())

# COMMAND ----------

stream_df.writeStream \
  .option('checkpointLocation', checkpoint_location) \
  .trigger(availableNow=True) \
  .foreachBatch(write_df) \
  .start()