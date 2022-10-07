# Databricks notebook source
# MAGIC %md 
# MAGIC Live table reads<br>
# MAGIC Use `dlt.read("table")` or `spark.table("LIVE.table")`to perform a complete read from a dataset defined in the same pipeline. 
# MAGIC 
# MAGIC Streaming Read<br>
# MAGIC Use `dlt.read_stream("table")` to perform a streaming read from a dataset defined in the same pipeline.

# COMMAND ----------

import dlt

@dlt.view
def taxi_raw():
  return spark.read.format("json") \
    .load("/databricks-datasets/nyctaxi/sample/json/")

# COMMAND ----------

@dlt.table
def taxi_raw_copy_table():
  return dlt.read("taxi_raw")

@dlt.table
def taxi_raw_copy_table_min():
  return spark.table("LIVE.taxi_raw_copy_table").limit(10)

# COMMAND ----------

@dlt.table(name="multi_pass")
def create_filtered_data():
  return dlt.read("taxi_raw_copy_table_min").where("passenger_count > 2")
