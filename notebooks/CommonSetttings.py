# Databricks notebook source
pip install git+https://github.com/databricks/databricks-cli.git@tunneling-cli

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024*1024*1024)
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set( "spark.sql.shuffle.partitions", "auto" )

sc.setJobDescription("JOB_DESCRIPTION") # ?? not sure if used
spark.conf.set("spark.job.description", "MIKE_TEST_JOB")

sc.setLocalProperty("callSite.short", "STAGE1")
sc.setLocalProperty("callSite.long", "STAGE2")

sc.defaultParallelism

# COMMAND ----------

df = spark.range(10000).toDF("id").repartition(16)
display(df)

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)

# Set the following on the clusters
spark.databricks.io.cache.maxDiskUsage 50g
spark.databricks.io.cache.maxMetaDataCache 1g
spark.databricks.io.cache.compression.enabled false

# COMMAND ----------

spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE mikem.bchmrk_rslts SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5'
# MAGIC )