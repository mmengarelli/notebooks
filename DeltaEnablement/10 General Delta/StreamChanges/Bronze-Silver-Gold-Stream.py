# Databricks notebook source
dbutils.fs.rm("/tmp/delta/_checkpoints/1", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC set db_name = mikem_stream_delta;
# MAGIC drop database if exists ${hiveconf:db_name} cascade;
# MAGIC create database ${hiveconf:db_name};
# MAGIC use ${hiveconf:db_name};

# COMMAND ----------

import string, random
import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType

def id_generator(size, chars=string.ascii_uppercase + string.digits):
  return ''.join(random.choice(chars) for _ in range(size))

def generate_date(run_records):
  spec = dg.DataGenerator(spark, rows=run_records) \
    .withIdOutput() \
    .withColumn("year", IntegerType(), minValue=2020, maxValue=2022) \
    .withColumn("month", IntegerType(), minValue=1, maxValue=12) \
    .withColumn("int1", IntegerType(), minValue=1, maxValue=9999) \
    .withColumn("int2", IntegerType(), minValue=1, maxValue=999) \
    .withColumn("other1", StringType(), values=['a', 'b', 'c']) \
    .withColumn("loong2", StringType(), values=[id_generator(500)]) 
  return spec.build()

sql("use mikem_stream_delta")

# COMMAND ----------

df = generate_date(50)

df.write.partitionBy("year", "month") \
  .mode("append") \
  .saveAsTable("bronze")

# COMMAND ----------

display(sql("select count(1) from bronze"))

# COMMAND ----------

bronze_df = spark.readStream.format("delta").table("bronze")

bronze_df.writeStream \
  .queryName("write_silver") \
  .option("checkpointLocation", "/tmp/delta/_checkpoints/1/silver") \
  .trigger(once=True) \
  .toTable("silver")

# COMMAND ----------

display(sql("select count(1) from silver"))

# COMMAND ----------

silver_df = spark.readStream.format("delta").table("silver")

silver_df.writeStream \
  .queryName("write_gold") \
  .option("checkpointLocation", "/tmp/delta/_checkpoints/1/gold") \
  .trigger(once=True) \
  .toTable("gold")

# COMMAND ----------

display(sql("select count(1) from gold"))

# COMMAND ----------


