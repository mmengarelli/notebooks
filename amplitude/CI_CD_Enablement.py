# Databricks notebook source
# MAGIC %md #### Browse mock **Amplitude** data

# COMMAND ----------

# DBTITLE 1,DBFS Mounts on S3
# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %fs ls /mnt/mikem/amplitude

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/mnt/mikem/amplitude/")
#display(df)

# COMMAND ----------

newDF = df.withColumnRenamed("$insert_id", "id").drop("$schema").drop("event_properties")
newDF.write.format("delta").mode("overwrite").saveAsTable("mikem.amplitude")

print("heya world")

# COMMAND ----------

# MAGIC %sql select * from mikem.amplitude