# Databricks notebook source
import dbldatagen as dg
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT db DEFAULT "default";
# MAGIC CREATE WIDGET TEXT chkpt_location DEFAULT "default";

# COMMAND ----------

spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', 'true')

# COMMAND ----------

db = dbutils.widgets.get("db")

# COMMAND ----------

spark.sql(f"""create table if not exists {db}.cdf_blog_item_purchase(purchase_id bigint, item_id string, shipping_st string, purchase_time timestamp)
tblproperties(delta.enableChangeDataCapture=true)""")

spark.sql(f"create table if not exists {db}.cdf_blog_item_purchase_agg_wcdf(item_id string, shipping_st string, week_starting date, num_sold int)")
spark.sql(f"create table if not exists {db}.cdf_blog_item_purchase_agg_wocdf(item_id string, shipping_st string, week_starting date, num_sold int)")

# COMMAND ----------

# DBTITLE 1,Initial Data
data_rows = 1000 * 100 * 100 * 100
partitions_requested = 100
dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
           .withColumn("purchase_id", "long",uniqueValues=data_rows)
            .withColumn("item_id", 'string', values=['x1','zd-54','cx-99','bt-734'])
            .withColumn("shipping_st", 'string', values=['IL','ID','TX','MI'])
            .withColumn("purchase_time", "timestamp",begin="2021-12-15 03:00:00", end="2022-01-31 03:00:00", interval="10 minutes", random=True)
           )

df1 = dataspec.build()
# display(df1)
df1.write.mode("overwrite").saveAsTable(f"{db}.cdf_blog_item_purchase")

# COMMAND ----------

# MAGIC %md
# MAGIC # Update some data after running 02_Aggregate for the first time. Compare the load times of complete AGG vs incremental

# COMMAND ----------

spark.sql(f"""update {db}.cdf_blog_item_purchase set shipping_st = 'ID' where purchase_id=1""")
spark.sql(f"""insert into {db}.cdf_blog_item_purchase values (
(20000000001, 'x1', 'IL', '2022-03-01')
)""")
spark.sql(f"delete from {db}.cdf_blog_item_purchase where purchase_id=1")

# COMMAND ----------

display(spark.sql(f"select * from table_changes('{db}.cdf_blog_item_purchase',2)"))

# COMMAND ----------


