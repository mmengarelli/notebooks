# Databricks notebook source
evt_log_path = "/pipelines/f66cded4-0db2-4dec-a026-87a9e411375c/system/events"

# COMMAND ----------

display(dbutils.fs.ls("/pipelines/f66cded4-0db2-4dec-a026-87a9e411375c/system/events"))

# COMMAND ----------

event_log = spark.read.format("delta").load(evt_log_path)
event_log.createOrReplaceTempView("event_log")

# COMMAND ----------

# MAGIC %sql desc event_log

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct event_type from event_log

# COMMAND ----------

# MAGIC %sql
# MAGIC select origin.update_id 
# MAGIC from event_log 
# MAGIC where event_type = 'create_update' 
# MAGIC ORDER BY timestamp DESC 
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,Audit logs
# MAGIC %sql 
# MAGIC select timestamp, details:user_action:action, details:user_action:user_name 
# MAGIC from event_log
# MAGIC where event_type = 'user_action'

# COMMAND ----------

# DBTITLE 1,Lineage
# MAGIC %sql 
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log
# MAGIC WHERE event_type = 'flow_definition' 

# COMMAND ----------

# DBTITLE 1,Data quality
# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name

# COMMAND ----------

# DBTITLE 1,Cluster performance metrics
# MAGIC %sql 
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   Double(details :cluster_utilization.num_executors) as current_num_executors,
# MAGIC   Double(details :cluster_utilization.avg_num_task_slots) as avg_num_task_slots,
# MAGIC   Double(
# MAGIC     details :cluster_utilization.avg_task_slot_utilization
# MAGIC   ) as avg_task_slot_utilization,
# MAGIC   Double(
# MAGIC     details :cluster_utilization.avg_num_queued_tasks
# MAGIC   ) as queue_size,
# MAGIC   Double(details :flow_progress.metrics.backlog_bytes) as backlog
# MAGIC FROM
# MAGIC   event_log_raw
# MAGIC WHERE
# MAGIC   event_type IN ('cluster_utilization', 'flow_progress')
# MAGIC   AND origin.update_id = '${latest_update.id}'
