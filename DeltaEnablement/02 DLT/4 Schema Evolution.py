# Databricks notebook source
import dlt

@dlt.view
def vw_raw():
  cols = ["col1","col2", "col3"]
  rows = [(1,1,1), (2,2,2), (3,3,3)]
  return spark.createDataFrame(rows, cols)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col,lit

@dlt.table
def dummy_silver():
  return spark.table("LIVE.vw_raw").withColumn("ts", lit(1))
