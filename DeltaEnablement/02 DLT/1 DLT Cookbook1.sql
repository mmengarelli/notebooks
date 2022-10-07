-- Databricks notebook source
-- MAGIC %python
-- MAGIC def square(i: int) -> int:
-- MAGIC   return i * i
-- MAGIC 
-- MAGIC spark.udf.register("makeItSquared", square)

-- COMMAND ----------

CREATE LIVE TABLE raw_squared AS SELECT makeItSquared(2) AS numSquared;

-- COMMAND ----------

CREATE LIVE TABLE input_data AS
SELECT "2021/09/04" AS date, 22.4 as sensor_reading UNION ALL
SELECT "2021/09/05" AS date, 21.5 as sensor_reading

-- COMMAND ----------


