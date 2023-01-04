# Databricks notebook source
# MAGIC %md # Data Generator
# MAGIC Generates parquet files with dummy data in various partitions<br>
# MAGIC 
# MAGIC For testing purposes only
# MAGIC 
# MAGIC Ex: `dbfs:/mnt/mikem/data/autoloader_test1/Source=src1/Database=db1/Table=tbl1/Year=2022/Month=1/Day=2/Time=4/part-00000-e117d57e-de3d-47ac-b7aa-26024cdf81ed.c000.snappy.parquet`

# COMMAND ----------

path = dbutils.widgets.get("path")
dbutils.fs.rm(path, True)
print(f"Path is: {path}")

# COMMAND ----------

spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
spark.conf.set("parquet.enable.summary-metadata", "false")

cols = [
    "Id",
    "Seq",
    "Value",
    "Source",
    "Database",
    "Table",
    "Year",
    "Month",
    "Day",
    "Time"
]

def generate_data_frame(i,y,mon,day,time):
  rows = [(i,1,"dummy","src1","db1","tbl1",y,mon,day,time)]
  return spark.createDataFrame(rows, cols)

def generate_and_save(i,y,m,d,t):
  df = generate_data_frame(i,y,m,d,t)
  df.coalesce(1).write \
    .partitionBy("Year", "Month", "Day", "Time") \
    .mode("append") \
    .parquet(path)
  print(f"Wrote to {path}")

# COMMAND ----------

# Generates a parquet file (see above) to feed autoloader 
# Pauses then creates a new one in a new partition 
import time
import random

id = 0
for m in range(1,2): # month
  for d in range (1,3): # day 
    for t in range (1,4): # time
      print(f"Generating ../{m}/{d}/{t}")
      generate_and_save(random.randint(1,999),2022,m,d,t)
      time.sleep(2) # wait

# COMMAND ----------

# dbutils.fs.rm(path, True)
# dbutils.fs.rm("/mnt/mikem/chkpts/AutoLoader/autoloader5", True)
# generate_and_save(1,2020,1,1,10)

# COMMAND ----------

# dbutils.fs.rm("/mnt/mikem/data/autoloader1", True)
# dbutils.fs.rm("/mnt/mikem/data/autoloader2", True)
# dbutils.fs.rm("/mnt/mikem/data/autoloader3", True)
# dbutils.fs.rm("/mnt/mikem/data/autoloader4", True)
# dbutils.fs.rm("/mnt/mikem/data/autoloader5", True)

# dbutils.fs.rm("/mnt/mikem/chkpts/AutoLoader", True)
# dbutils.fs.mkdirs("/mnt/mikem/chkpts/AutoLoader")

# sql("drop database if exists mikem_autoloaders cascade")
# sql("create database mikem_autoloaders")