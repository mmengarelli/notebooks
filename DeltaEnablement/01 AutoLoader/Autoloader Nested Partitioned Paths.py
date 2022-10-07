# Databricks notebook source
root = "/mnt/mikem/data/root"
chkpt = "/mnt/mikem/chkpts/alnpp"
schema_location = "/mnt/mikem/data/schema/alnpp"
sql("drop table if exists mikem.alnpp")

# COMMAND ----------

dbutils.fs.mkdirs(root)

# COMMAND ----------

def gen_data(a):
  df = spark.createDataFrame([(a, a)], ["col1","col2"])
  df.coalesce(1).write.mode("overwrite").json(f"{root}/{a}")
  print(f"created {root}/{a}")

# COMMAND ----------

gen_data('d')

# COMMAND ----------

stream_df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", schema_location) \
  .load(root)

# COMMAND ----------

stream_df.writeStream \
  .outputMode("append") \
  .option("checkpointLocation", chkpt) \
  .toTable("mikem.alnpp")

# COMMAND ----------

df = sql("select * from mikem.alnpp")
display(df)
