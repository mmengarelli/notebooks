# Databricks notebook source
import dlt
from pyspark.sql.functions import lit

path = spark.conf.get("path.name")

@dlt.table(name="dummy")
def build():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load(path)
  )
