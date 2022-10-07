-- Databricks notebook source
-- MAGIC %md # Intro
-- MAGIC 
-- MAGIC `"APPLY CHANGES"`
-- MAGIC 
-- MAGIC To use Delta Live Tables CDC, you must enable the feature in each pipeline by adding the following configuration to the pipeline settings:
-- MAGIC 
-- MAGIC <pre>{
-- MAGIC   "configuration": {
-- MAGIC     "pipelines.applyChangesPreviewEnabled": "true"
-- MAGIC   }
-- MAGIC }</pre>
-- MAGIC 
-- MAGIC ## SQL 
-- MAGIC Use the `APPLY CHANGES INTO` statement to use Delta Live Tables CDC functionality:
-- MAGIC <pre>
-- MAGIC APPLY CHANGES INTO table
-- MAGIC FROM source
-- MAGIC KEYS (keys)
-- MAGIC [WHERE condition]
-- MAGIC SEQUENCE BY orderByColumn
-- MAGIC </pre>
-- MAGIC 
-- MAGIC Parameters:
-- MAGIC * KEYS - The column(s) that uniquely identify a row in the source data. This is used to identify which CDC events apply to specific records in the target table.
-- MAGIC * WHERE (optional) - Condition applied to both source and target to trigger optimizations such as partition pruning.
-- MAGIC * IGNORE NULL UPDATES (optional)  - When a CDC event matches an existing row and IGNORE NULL UPDATES is specified, columns with a null will retain their existing values in the target. This also applies to nested columns with a value of null.
-- MAGIC * APPLY AS DELETE WHEN - Specifies when a CDC event should be treated as a DELETE rather than an upsert. 
-- MAGIC * SEQUENCE BY - The column name specifying the logical order of CDC events in the source data. Used to handle out of order data 
-- MAGIC * COLUMNS - Specifies a subset of columns to include in the target table
-- MAGIC 
-- MAGIC The default behavior for INSERT and UPDATE events is to upsert CDC events from the source: update any rows in the target table that match the specified key(s) or insert a new row when a matching record does not exist in the target table. Handling for DELETE events can be specified with the APPLY AS DELETE WHEN condition.
-- MAGIC 
-- MAGIC 
-- MAGIC ## Python
-- MAGIC <pre>
-- MAGIC apply_changes(
-- MAGIC   target = "<target-table>",
-- MAGIC   source = "<data-source>",
-- MAGIC   keys = ["key1", "key2", "keyN"],
-- MAGIC   sequence_by = "<sequence-column>",
-- MAGIC   ignore_null_updates = False,
-- MAGIC   apply_as_deletes = None,
-- MAGIC   column_list = None,
-- MAGIC   except_column_list = None
-- MAGIC )
-- MAGIC </pre>

-- COMMAND ----------

-- DBTITLE 1,Generate test data
DROP SCHEMA IF EXISTS mikem_cdc_data CASCADE;
CREATE SCHEMA IF NOT EXISTS mikem_cdc_data;

CREATE TABLE mikem_cdc_data.users
AS SELECT
  col1 AS userId,
  col2 AS name,
  col3 AS city,
  col4 AS operation,
  col5 AS sequenceNum
FROM (
  VALUES
  -- Initial load.
  (123, "Isabel",   "Monterrey",   "INSERT", 1),
  (124, "Raul",     "Oaxaca",      "INSERT", 1),
  -- One new user.
  (125, "Mercedes", "Tijuana",     "INSERT", 2),
  -- Isabel is removed from the system and Mercedes moved to Guadalajara.
  (123, null,       null,          "DELETE", 5),
  (125, "Mercedes", "Guadalajara", "UPDATE", 5),
  -- This batch of updates arrived out of order. The above batch at sequenceNum 5 will be the final state.
  (123, "Isabel",   "Chihuahua",   "UPDATE", 4),
  (125, "Mercedes", "Mexicali",    "UPDATE", 4)
);

select * from mikem_cdc_data.users;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import dlt
-- MAGIC from pyspark.sql.functions import col, expr
-- MAGIC 
-- MAGIC @dlt.view
-- MAGIC def users():
-- MAGIC   return spark.readStream.format("delta").table("mikem_cdc_data.users")
-- MAGIC 
-- MAGIC dlt.create_target_table("target")
-- MAGIC 
-- MAGIC dlt.apply_changes(
-- MAGIC   target = "target",
-- MAGIC   source = "users",
-- MAGIC   keys = ["userId"],
-- MAGIC   sequence_by = col("sequenceNum"),
-- MAGIC   apply_as_deletes = expr("operation = 'DELETE'"),
-- MAGIC   except_column_list = ["operation", "sequenceNum"]
-- MAGIC )

-- COMMAND ----------

create or refresh live table user_source as select * from mikem_cdc_data.users

-- COMMAND ----------

APPLY CHANGES INTO LIVE.user_source
FROM mikem_cdc_data.users
KEYS (userId)
SEQUENCE BY sequenceNum
--[COLUMNS {columnList | * EXCEPT (exceptColumnList)}]
