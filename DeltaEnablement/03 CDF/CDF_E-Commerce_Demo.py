# Databricks notebook source
# MAGIC %md ---
# MAGIC title: CDF E-Comerce Demo
# MAGIC authors:
# MAGIC - Alex Ott
# MAGIC - Sergio Ballesteros
# MAGIC 
# MAGIC tags:
# MAGIC  - python
# MAGIC  - cdf
# MAGIC  - delta
# MAGIC  - change-data-feed
# MAGIC  - streaming
# MAGIC  - change-data-capture
# MAGIC  - cdc
# MAGIC  - pipelines
# MAGIC  - etl
# MAGIC  - composable-pipelines
# MAGIC 
# MAGIC * created_at: 2021-10-06
# MAGIC * updated_at: 2021-10-06
# MAGIC 
# MAGIC tldr: Demonstrates the use of CDF with streaming using Delta Lake
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Links
# MAGIC - AWS demo.cloud: [https://demo.cloud.databricks.com/#notebook/11573000](https://demo.cloud.databricks.com/#notebook/11573000)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Change Data Feed Demo

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This notebook demonstrates the use of CDF for purposes of E-Commerce.  In typical workflows, there could be situations when we need to perform changes in the data:
# MAGIC * We receive request to delete user's data, including transactions. This is a common scenario to be GDPR compliant. In this case we need to remove all data in bronze & silver layers (we can mask personal data instead as well), but this shouldn't affect aggregations at the gold layer, such as, daily/monthly amounts, etc.
# MAGIC * We receive transaction correction requests - for example, discount wasn't applied, or an item was returned. In this case we need to append this request to the bronze layer, make correction of specific transaction in the silver layer & update daily/monthly aggregations to reflect differences in the amount.
# MAGIC 
# MAGIC All processing is done in the streaming fashion.
# MAGIC 
# MAGIC The following diagram shows the architecture of the demo with the different tables and functions to process data:
# MAGIC <img src="https://raw.githubusercontent.com/sergioballesterossolanas/demo_resources/master/demo_cdf_arch.png" width="1000"/>
# MAGIC 
# MAGIC We can see that the tables and functions to process the data are divided into boxes or domains. In this we could assume that there is one team responsible for each domain. For the sake of simplicity all the processes are done in this notebook, however in practice these processes could happen in different notebooks or pipelines.
# MAGIC 
# MAGIC We can see that each team is responsible for reading tables and allowing others to read the tables that they own. This pattern resembles to the data mesh architectural pattern.

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, Row
from delta.tables import *
import random
import string

# COMMAND ----------

# MAGIC %md
# MAGIC ## First let us create a unique database for the bronze, silver and gold tables

# COMMAND ----------

# Create a unique database sufix to reuse this notebook
suffix_db = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

# COMMAND ----------

# Create database
db_name = "cdf_ecommerce_{}".format(suffix_db)
spark.sql(f"drop database if exists {db_name}")
spark.sql(f"create database if not exists {db_name}")
spark.sql(f"use {db_name}")
print("Using database", db_name)

# COMMAND ----------

# Create checkpoints
checkpoints_dir = "/tmp/aott-cdf-demo/checkpoints_{}".format(suffix_db)
dbutils.fs.rm(checkpoints_dir, True)
dbutils.fs.mkdirs(checkpoints_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Bronze layer
# MAGIC We will create a orders_bronze table, that:
# MAGIC - Gets new rows (append) when there are new orders or an older order is updated
# MAGIC - Contains personal data, which will be masked upon user request

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists orders_bronze;
# MAGIC create table if not exists orders_bronze (
# MAGIC   order_id int,
# MAGIC   time timestamp,
# MAGIC   user_id int,
# MAGIC   address_id int,
# MAGIC   payment_id int,
# MAGIC   order array<struct<item:string, number:int, price:float>>,
# MAGIC   total float, -- when doing correction, this will be a new price for 'update', or negative price on 'return'.  
# MAGIC   -- Potentially we don't need total here, we can calculate it in the silver layer
# MAGIC   operation string -- type of operation: 'new' for new transactions, 'update' - for corrections, like applying discount, 'return', 'user_delete', etc.
# MAGIC ) using delta
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into orders_bronze values 
# MAGIC (2, '2021-07-21T02:12:00Z', 2, 1, 1, array(struct('i2', 1, 20.0)), 20.0, 'new');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into orders_bronze values 
# MAGIC (3, '2021-07-20T02:12:00Z', 3, 0, 0, array(struct('i3', 1, 30.0)), 30.0, 'new');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into orders_bronze values 
# MAGIC (3, '2021-07-20T02:12:00Z', 3, 0, 0, array(struct('i3', 1, 30.0)), 30.0, 'return');

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### User data table
# MAGIC This table contains user data, such as user name, address and payment information

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists users;
# MAGIC create table if not exists users (
# MAGIC   user_id int,
# MAGIC   firstname string,
# MAGIC   lastname string,
# MAGIC   addresses array<string>,
# MAGIC   payment_info array<string>
# MAGIC ) using delta
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC insert into users values 
# MAGIC   (1, 'F1', 'L1', array('address1-1', 'address1-2'), array('payment1-1', 'payment1-2')),
# MAGIC   (2, 'F2', 'L2', array('address2-1', 'address2-2'), array('payment2-1', 'payment2-2')),
# MAGIC   (3, 'F3', 'L3', array('address3-1', 'address3-2'), array('payment3-1', 'payment3-2'));

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from users

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- emulate deletion of user
# MAGIC delete from users where user_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from users

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Handling deletion of users
# MAGIC 
# MAGIC When user is deleted from the `users` table, we're updating `order_bronze` table, and allow to propagate changes to silver `orders` table

# COMMAND ----------

def merge_users_delete(df: DataFrame, batch_id: int):
  ndf = df.filter("_change_type = 'delete'") # ignoring everything except deletes
 
  deltaTable = DeltaTable.forName(spark, f"{db_name}.orders_bronze")
  deltaTable.alias("t").merge(
    ndf.alias("u"), "t.user_id = u.user_id")\
    .whenMatchedUpdate(set = { "t.user_id" : "-1", "t.operation": "'user_delete'", "t.address_id": "-1", "t.payment_id": "-1"})\
    .execute()

# COMMAND ----------

dbutils.fs.rm(f"{checkpoints_dir}/users_delete", True)
user_deletes_df = spark.readStream.format("delta")\
    .option("readChangeData", "true")\
    .option("startingVersion", 0)\
    .table("users")\
    .writeStream\
    .option("checkpointLocation", f"{checkpoints_dir}/users_delete")\
    .outputMode("update")\
    .foreachBatch(merge_users_delete)\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Silver `orders` table
# MAGIC This table contains clean version of `order_bronze`. This means that when orders are updated, on `order_bronze` they are new rows, but on `order` the original row is updated.

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists orders;
# MAGIC create table if not exists orders (
# MAGIC   order_id int,
# MAGIC   time timestamp,
# MAGIC   user_id int,
# MAGIC   order array<struct<item:string, number:int, price:float>>,
# MAGIC   total float,
# MAGIC   firstname string,
# MAGIC   lastname string,
# MAGIC   address string,
# MAGIC   payment_info string
# MAGIC ) using delta
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Handling changes in the orders_bronze table
# MAGIC 
# MAGIC This includes:
# MAGIC * new orders: `operation = new`
# MAGIC * corrections: `operation = update`
# MAGIC * returns: `operation = return` (return could be partial)
# MAGIC * user deletes: `operation = user_delete`

# COMMAND ----------

pii_columns = ["firstname", "lastname", "address", "payment_info"]

def merge_bronze_orders(df: DataFrame, batch_id: int):
  ndf = df.filter("_change_type = 'insert' or _change_type = 'update_postimage'") # ignoring everything except deletes
  df1 = ndf.filter("_change_type = 'insert'")
  users = spark.read.table(f"{db_name}.users")
  jdf = df1.join(users, ["user_id"])
  jdf.printSchema()
  jdf = jdf.select("operation", "order_id", "time", "user_id", "order", "total", "firstname", "lastname", 
            F.col("addresses").getItem(F.col("address_id")).alias("address"), 
            F.col("payment_info").getItem(F.col("payment_id")).alias("payment_info"))
  df2 = ndf.filter("_change_type = 'update_postimage' and user_id = -1")\
    .select("operation", "order_id", "time", "user_id", "order", "total", *[F.lit("REMOVED").alias(cl) for cl in pii_columns])
  adf = jdf.unionByName(df2)
  deltaTable = DeltaTable.forName(spark, f"{db_name}.orders")
  all_cols_wo_op = [cl for cl in adf.columns if cl != 'operation']
  deltaTable.alias("t").merge(
    adf.alias("u"), "t.order_id = u.order_id")\
    .whenMatchedUpdate(condition = "u.operation = 'user_delete'", set = { "t.user_id" : "u.user_id", **dict([(f"t.{cl}", f"u.{cl}") for cl in pii_columns]) } )\
    .whenMatchedUpdate(condition = "u.operation = 'update'", set = dict([(f"t.{cl}", f"u.{cl}") for cl in all_cols_wo_op]))\
    .whenMatchedUpdate(condition = "u.operation = 'return'", set = {"t.total": "t.total - u.total"})\
    .whenNotMatchedInsert(condition = "u.operation = 'new'", values = dict([(cl, f"u.{cl}") for cl in all_cols_wo_op]))\
    .execute() 

# COMMAND ----------

dbutils.fs.rm(f"{checkpoints_dir}/bronze_orders", True)
user_deletes_df = spark.readStream.format("delta")\
    .option("readChangeData", "true")\
    .option("startingVersion", 0)\
    .table(f"{db_name}.orders_bronze")\
    .writeStream\
    .option("checkpointLocation", f"{checkpoints_dir}/bronze_orders")\
    .outputMode("update")\
    .foreachBatch(merge_bronze_orders)\
    .start()

# COMMAND ----------

display(spark.read.table(f"{db_name}.orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Gold layer
# MAGIC 
# MAGIC Gold layer includes daily sales aggregations that come from the Silver layer as:
# MAGIC * Inserts - just normal sales
# MAGIC * Updates - corrections to sales
# MAGIC * Deletes - should be ignored
# MAGIC 
# MAGIC All operations are performed on hourly base, updating the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists daily_sales;
# MAGIC create table if not exists daily_sales (
# MAGIC   date date,
# MAGIC   amount double
# MAGIC ) using delta;

# COMMAND ----------

def merge_daily_sales(df: DataFrame, batch_id: int):
  ndf = df.filter("_change_type <> 'delete'") # ignoring deletes in the upstream table
  ndf = ndf.withColumn("total", F.when(F.col("_change_type") == "update_preimage", -F.col("total")).otherwise(F.col("total")))\
    .groupBy(F.to_date(F.col("time")).alias("date")).agg(F.sum("total").alias("amount"))
  
  deltaTable = DeltaTable.forName(spark, f"{db_name}.daily_sales")
  deltaTable.alias("t").merge(
    ndf.alias("u"), "t.date = u.date")\
    .whenMatchedUpdate(set = { "amount" : "u.amount + t.amount" } )\
    .whenNotMatchedInsertAll()\
    .execute()

# COMMAND ----------

dbutils.fs.rm(f"{checkpoints_dir}/daily_sales", True)
orders_df = spark.readStream.format("delta")\
    .option("readChangeData", "true")\
    .option("startingVersion", 0)\
    .table("orders")\
    .writeStream\
    .option("checkpointLocation", f"{checkpoints_dir}/daily_sales")\
    .outputMode("update")\
    .foreachBatch(merge_daily_sales)\
    .start()

# COMMAND ----------

display(spark.read.table("daily_sales"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean resources
# MAGIC Run the following cell to delete the created tables, database and checkpoints

# COMMAND ----------

spark.sql("drop table if exists orders")
spark.sql("drop table if exists orders_bronze")
spark.sql("drop table if exists users")
spark.sql("drop table if exists daily_sales")
spark.sql(f"drop database if exists {db_name}")
dbutils.fs.rm(checkpoints_dir, True)
