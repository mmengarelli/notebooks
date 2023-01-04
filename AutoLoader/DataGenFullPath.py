# Databricks notebook source
# MAGIC %md # Data Generator
# MAGIC Generates parquet files in various partitions<br>
# MAGIC Ex: `/raw/Source=Parcel_Pro/Database=InvoicePPI/Table=dbo.BillingDates/year=2022/month=Dec/day=22/time=09`

# COMMAND ----------

path = dbutils.widgets.get("path")
dbutils.fs.rm(path, True)

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
  return spark.createDataFrame(rows, cols, schema)

def generate_and_save(i,y,m,d,t):
  df = generate_data_frame(i,y,m,d,t)
  df.coalesce(1).write \
    .partitionBy("Source", "Database", "Table", "Year", "Month", "Day", "Time") \
    .mode("append") \
    .parquet(path)

# COMMAND ----------

# Generates a parquet file (see above) to feed autoloader 
# Pauses then creates a new one in a new partition 
import time
import random

id = 0
for m in range(1,5): # month
  for d in range (1,5): # day 
    for t in range (1, 5): # time
      print(f"Generating /{m}/{d}/{t}")
      generate_and_save(random.randint(1,999),2022,m,d,t)
      time.sleep(5) # wait

# COMMAND ----------

generate_and_save(1,9999,1,1,1)