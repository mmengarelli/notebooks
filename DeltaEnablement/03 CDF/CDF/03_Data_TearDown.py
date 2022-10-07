# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT db DEFAULT "default";
# MAGIC CREATE WIDGET TEXT chkpt_location DEFAULT "default";

# COMMAND ----------

db = dbutils.widgets.get("db")

# COMMAND ----------

spark.sql(f"drop table if  exists {db}.cdf_blog_item_purchase")
spark.sql(f"drop table if  exists  {db}.cdf_blog_item_purchase_agg_wcdf")
spark.sql(f"drop table if  exists  {db}.cdf_blog_item_purchase_agg_wocdf")

# COMMAND ----------

# DBTITLE 1,Initial Data
dbutils.fs.rm(dbutils.widgets.get("chkpt_location"), recurse=True)

# COMMAND ----------


