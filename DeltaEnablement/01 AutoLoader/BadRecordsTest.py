# Databricks notebook source
path = "/mnt/mikem/data/dummy99999/"
chkpt_path = "/mnt/mikem/checkpoints/dummy99999"
write_path = "/mnt/mikem/out/dummy99999/"

# COMMAND ----------

df = spark.read \
  .option("header", True) \
  .csv(path)

df.show()

# COMMAND ----------

schema = spark.read.option("header", True).csv(path).schema

# COMMAND ----------

df = spark.readStream \
  .option("header", True) \
  .format('csv') \
  .schema(schema) \
  .load(path)

display(df)

# COMMAND ----------

string_schema = "id string, key string, val string, date string"

correct_stream_df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .option('cloudFiles.schemaLocation', '/tmp/schema') \
  .option("rescuedDataColumn", "__rescue_data") \
  .schema(string_schema) \
  .load(path)

# COMMAND ----------

display(correct_stream_df)

# COMMAND ----------

dbutils.fs.rm(chkpt_path, True)
dbutils.fs.rm(write_path, True)

correct_stream_df.writeStream.format('delta') \
  .option('checkpointLocation', chkpt_path) \
  .start(write_path)

# COMMAND ----------

display(dbutils.fs.ls(write_path))

# COMMAND ----------

df = spark.read.format('delta').load(write_path)
display(df)
