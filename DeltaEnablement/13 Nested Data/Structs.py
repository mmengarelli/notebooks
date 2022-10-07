# Databricks notebook source
# MAGIC %sql
# MAGIC use mikem;
# MAGIC set table = employee_simple_struct;
# MAGIC 
# MAGIC drop table if exists ${hiveconf:table}; 
# MAGIC 
# MAGIC create table ${hiveconf:table}
# MAGIC (
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   employee_info struct<employer:string, eid:int, address:string>
# MAGIC );
# MAGIC 
# MAGIC desc extended ${hiveconf:table};

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled=false;
# MAGIC truncate table ${hiveconf:table};
# MAGIC 
# MAGIC insert into table ${hiveconf:table} values(1,'Bala', struct("databricks", 1, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(2,'Karen', struct("uber", 2, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(3,'Kim', struct("apple", 3, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(4,'Steve', struct("uber", 4, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(5,'Bob', struct("databricks", 5, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(6,'Vini', struct("uber", 6, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(7,'Bill', struct("databricks", 7, "160 spear st"));
# MAGIC insert into table ${hiveconf:table} values(8,'Van', struct("apple", 8, "160 spear st"));
# MAGIC 
# MAGIC optimize ${hiveconf:table};
# MAGIC vacuum ${hiveconf:table} retain 0 hours;

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/mikem.db/employee_simple_struct/

# COMMAND ----------

# MAGIC %sql describe history ${hiveconf:table};

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${hiveconf:table} 
# MAGIC where employee_info.employer = "uber"

# COMMAND ----------

# MAGIC %pip install parquet-tools

# COMMAND ----------

# MAGIC %sh 
# MAGIC export FILE=/dbfs/user/hive/warehouse/mikem.db/employee_simple_struct/part-00000-beba6d77-122c-4e5b-8fa6-b7febd8ea4d3-c000.snappy.parquet
# MAGIC 
# MAGIC # parquet-tools inspect --detail $FILE
# MAGIC # parquet-tools show $FILE 
# MAGIC parquet-tools inspect $FILE
