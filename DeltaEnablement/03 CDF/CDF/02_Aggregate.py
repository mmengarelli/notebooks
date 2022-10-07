# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT db DEFAULT "default";
# MAGIC CREATE WIDGET TEXT chkpt_location DEFAULT "default";

# COMMAND ----------

spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', 'true')

# COMMAND ----------

db = dbutils.widgets.get("db")
checkpoint = dbutils.widgets.get("chkpt_location")

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {db}.cdf_blog_item_purchase_agg_wocdf
select item_id, shipping_st, date_sub(purchase_time, dayOfWeek(purchase_time)-1) as week_starting, count(*) as num_sold
from {db}.cdf_blog_item_purchase
group by item_id, shipping_st, week_starting
""")

# COMMAND ----------

def upsertAgg(microBatchOutputDF, batchId):
  microBatchOutputDF.createOrReplaceTempView("updates")
  spark_batch = microBatchOutputDF._jdf.sparkSession()
  (spark_batch.sql("""select * from (
select
  item_id,
  shipping_st,
  date_sub(purchase_time, dayOfWeek(purchase_time)-1) as week_starting,
  sum(CASE
    WHEN _change_type = 'insert' THEN 1
    WHEN _change_type = 'delete' THEN -1
    WHEN _change_type = 'update_preimage' THEN -1
    WHEN _change_type = 'update_postimage' THEN 1
    ELSE 0
  END) as counter
from updates
group by item_id, shipping_st, week_starting
) """)
   .createOrReplaceTempView("condensed_updates")
  )
  
  spark_batch.sql(f"""
  MERGE INTO {db}.cdf_blog_item_purchase_agg_wcdf target
  using (select
  u.counter + coalesce(T.num_sold,0) as num_sold,
  u.item_id,
  u.shipping_st,
  u.week_starting
from
  condensed_updates u
  left join {db}.cdf_blog_item_purchase_agg_wcdf t on T.item_id = u.item_id and T.shipping_st = u.shipping_st and T.week_starting = u.week_starting) agg_update
  on agg_update.shipping_st = target.shipping_st and agg_update.item_id = target.item_id and agg_update.week_starting=target.week_starting 
  WHEN MATCHED and agg_update.num_sold = 0 THEN DELETE
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED and agg_update.num_sold <> 0 THEN INSERT *
  """)
  
(spark.readStream.format('delta')
 .option('readChangeFeed','true')
 .option('startingVersion',0)
 .table(f'{db}.cdf_blog_item_purchase')
 .writeStream.format('delta')
 .trigger(once=True)
 .option("checkpointLocation", checkpoint)
 .foreachBatch(upsertAgg)
 .start()

)

# COMMAND ----------

display(spark.sql(f"""
select * from (
select * from {db}.cdf_blog_item_purchase_agg_wcdf
except
select * from {db}.cdf_blog_item_purchase_agg_wocdf
)
union all(
select * from {db}.cdf_blog_item_purchase_agg_wocdf
except
select * from {db}.cdf_blog_item_purchase_agg_wcdf)
"""))

# COMMAND ----------


