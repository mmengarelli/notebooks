# Databricks notebook source
print("hello world")

df = spark.read.option("header",True).csv("/databricks-datasets/asa/airlines/") \
  .select("FlightNum", "Origin", "Dest", "DepTime", "ArrTime", "AirTime", "Distance") \
  .limit(99)

df.show()