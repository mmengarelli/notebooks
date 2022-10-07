-- Databricks notebook source
-- MAGIC %md 
-- MAGIC OPTIMIZE always creates new files. So if you run it on the source table it create new files there and the cloned one won't see them at all. If you run it on the cloned table it'll create new files there and the source table won't see them at all.
-- MAGIC 
-- MAGIC So yes, doing shallow clone + OPTIMIZE is a great way to explore different data layouts relatively cheaply
-- MAGIC 
-- MAGIC Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.
-- MAGIC 
-- MAGIC The metadata that is cloned includes: schema, partitioning information, invariants, nullability. 
-- MAGIC 
-- MAGIC For deep clones only, stream and COPY INTO metadata are also cloned. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("drop table if exists mikem.points_raw")
-- MAGIC 
-- MAGIC x = spark.range(8).toDF("x")
-- MAGIC y = spark.range(8).toDF("y")
-- MAGIC 
-- MAGIC points_raw = x.crossJoin(y)
-- MAGIC points_raw.sort("x", "y").write.option("maxRecordsPerFile", 4).saveAsTable("mikem.points_raw")
-- MAGIC display(points_raw)

-- COMMAND ----------

use mikem;
-- number of files read	8
select * from points_raw where y = 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("drop table if exists mikem.points_z")
-- MAGIC 
-- MAGIC x = spark.range(8).toDF("x")
-- MAGIC y = spark.range(8).toDF("y")
-- MAGIC 
-- MAGIC points_raw = x.crossJoin(y)
-- MAGIC points_raw.sort("x", "y").write.option("maxRecordsPerFile", 4).saveAsTable("mikem.points_z")
-- MAGIC sql("optimize mikem.points_z zorder by y")

-- COMMAND ----------

-- number of files read	1
select * from points_z where y = 2

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df = spark.range(5).toDF("id")

-- COMMAND ----------

drop table if exists points_shallow;
create table points_shallow shallow clone points_raw;
-- Wont affect source 
optimize points_shallow zorder by y;
describe extended points_shallow;

-- COMMAND ----------

select * from points_shallow where y = 2

-- COMMAND ----------


