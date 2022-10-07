# Databricks notebook source
# MAGIC %sql
# MAGIC set db_name = mikem_skew_test;
# MAGIC drop database if exists ${hiveconf:db_name} cascade;
# MAGIC create database ${hiveconf:db_name};

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.floor
# MAGIC 
# MAGIC sql("use mikem_skew_test")
# MAGIC 
# MAGIC val skewedRightSeq = 
# MAGIC   0.to(79).seq ++ 
# MAGIC   40.to(79).seq ++ 
# MAGIC   60.to(79).seq ++ 
# MAGIC   70.to(79).seq ++ 
# MAGIC   75.to(79).seq
# MAGIC 
# MAGIC val df = skewedRightSeq.toDF("val").withColumn("part", floor('val / 10))
# MAGIC df.repartition(4).write.mode("overwrite").partitionBy("part").saveAsTable("left")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --select * from ${hiveconf:db_name}.left
# MAGIC select count(1) as ct, val from ${hiveconf:db_name}.left
# MAGIC group by val
# MAGIC order by ct desc

# COMMAND ----------

# MAGIC %scala
# MAGIC val normSeq = 0.to(999999).seq 
# MAGIC 
# MAGIC val df = normSeq.toDF("val").withColumn("part", floor('val / 10))
# MAGIC df.write.mode("overwrite").partitionBy("part").saveAsTable("right")

# COMMAND ----------

# MAGIC %sql
# MAGIC use ${hiveconf:db_name};
# MAGIC 
# MAGIC select * from right r
# MAGIC left outer join left l on l.val = r.val
