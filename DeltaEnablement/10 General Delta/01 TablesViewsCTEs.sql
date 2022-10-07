-- Databricks notebook source
drop database if exists enb cascade;
create database enb;
use enb;
show tables;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.range(1,1000).toDF("id").write.saveAsTable("enb.source")

-- COMMAND ----------

create table t1 as select * from enb.source;

-- creates a `virtual` table that has no physical data based on the result-set of a SQL query
create view v1 as select * from enb.source;

-- temp views are session-scoped and is dropped when session ends because it skips persisting the definition in the underlying metastore, if any.
create temp view tv1 as select * from enb.source;

-- global temp views are tied to a system preserved temporary schema global_temp.
create global temp view gtv1 as select * from enb.source;

show tables;

-- COMMAND ----------


