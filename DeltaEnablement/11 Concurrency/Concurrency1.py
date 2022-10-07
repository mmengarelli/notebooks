# Databricks notebook source
# MAGIC %pip install git+https://github.com/databrickslabs/dbldatagen

# COMMAND ----------

sql("drop database if exists concurrency_test cascade")
sql("create database concurrency_test")
sql("use concurrency_test")

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType

spec = dg.DataGenerator(spark, name="test_data_set1", rows=50, partitions=4) \
  .withIdOutput() \
  .withColumn("rand", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=2) \
  .withColumn("key", StringType(), values=['A', 'B', 'C', 'D']) \
  .withColumn("value", StringType(), values=["the", "and", "or", "an"], random=True)

df = spec.build()
df.repartition("key").write.mode("append").format("delta").saveAsTable("concurr1")

# COMMAND ----------

display(sql("select count(*) from concurr1"))

# COMMAND ----------

for x in range(10):
  sql("delete from concurr1 where value='b'")

# COMMAND ----------

for i in range(20):
  df.coalesce(1).write.mode("overwrite").format("delta").saveAsTable("concurr1")
