# Databricks notebook source
import dlt

@dlt.table(name="scratch")
def scratch():
    spark.conf.set("test", "test")
    res = spark.conf.get("test")
    
    return (
      spark.createDataFrame([res], "string").toDF("age")
    )
