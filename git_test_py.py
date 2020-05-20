# Databricks notebook source
print("hello world")

spark.read.csv("/databricks-datasets/asa/airlines/").limit(5).show()