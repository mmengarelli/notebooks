# Databricks notebook source
path = spark.conf.get("path.name")
dbutils.fs.rm(path, True)

# COMMAND ----------

import random as rand
from pyspark.sql.functions import lit 
from datetime import datetime

def random():
  return rand.randint(1, 99999999)

df = spark.range(1, 1000) \
  .toDF("id") \
  .withColumn("col1", lit(random())) \
  .withColumn("ts", lit(datetime.now().timestamp()))

df.coalesce(5).write.json(path)
