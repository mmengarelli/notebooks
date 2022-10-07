# Databricks notebook source
sql("use concurrency_test")

# COMMAND ----------

for x in range(10):
  sql("update concurr1 set key='b' where value='b'")

# COMMAND ----------


