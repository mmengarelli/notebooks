# Databricks notebook source
print("Hello from Databricks")

# COMMAND ----------

df = spark.read.option("header",True).csv("/databricks-datasets/asa/airlines/") \
  .select("FlightNum", "Origin", "Dest", "DepTime", "ArrTime", "AirTime", "Distance") \
  .limit(10) \
  .show()

print("adding more")

