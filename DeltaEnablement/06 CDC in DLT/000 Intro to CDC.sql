-- Databricks notebook source
-- MAGIC %md-sandbox # Overview
-- MAGIC 
-- MAGIC Update tables in your Delta Live Tables pipeline based on changes in source data. To learn how to record and query row-level change information for Delta tables, see Change data feed.
-- MAGIC 
-- MAGIC Takes a set of changes (inserts, updates, deletes)<br>
-- MAGIC 
-- MAGIC The default behavior for `INSERT` and `UPDATE` events is to upsert CDC events from the source: update any rows in the target table that match the specified key(s) or insert a new row when a matching record does not exist in the target table. Handling for `DELETE` events can be specified with the `APPLY AS DELETE WHEN` condition.
-- MAGIC 
-- MAGIC Currently only streaming source are supported (options to apply snapshots of batch tables coming)
-- MAGIC 
-- MAGIC <pre>
-- MAGIC apply changes into live.cities
-- MAGIC from stream(live.cities_updates)
-- MAGIC keys(id)
-- MAGIC sequence(ts)
-- MAGIC </pre>
-- MAGIC 
-- MAGIC Keys used to determine uniqueness (insert vs update/delete)
-- MAGIC 
-- MAGIC # Sequences
-- MAGIC * Timestamp field
-- MAGIC * Monotonically increasing sequence id 
-- MAGIC 
-- MAGIC <img src="https://mikem-docs.s3.us-west-2.amazonaws.com/img/CDC.png" style="height: 200px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>

-- COMMAND ----------

-- MAGIC %md # Syntax
-- MAGIC 
-- MAGIC ## Python
-- MAGIC Use the `apply_changes()` function to use Delta Live Tables CDC functionality. The Delta Live Tables Python CDC interface also provides the `create_target_table()` function. You can use this function to create the target table required by the `apply_changes()` function.
-- MAGIC 
-- MAGIC <pre>
-- MAGIC apply_changes(
-- MAGIC   target = "target",
-- MAGIC   source = "source",
-- MAGIC   keys = ["key1", "key2", "keyN"],
-- MAGIC   sequence_by = "col1",
-- MAGIC   ignore_null_updates = False,
-- MAGIC   apply_as_deletes = None,
-- MAGIC   column_list = None,
-- MAGIC   except_column_list = None
-- MAGIC )
-- MAGIC </pre>
-- MAGIC 
-- MAGIC <pre>
-- MAGIC create_target_table(
-- MAGIC   name = "table",
-- MAGIC   comment = "comment"
-- MAGIC   spark_conf={"key" : "value"},
-- MAGIC   table_properties={"key" : "value"},
-- MAGIC   partition_cols=["col1, col2"],
-- MAGIC   path="/mnt/tables",
-- MAGIC   schema="schema-definition"
-- MAGIC )
-- MAGIC </pre>
-- MAGIC 
-- MAGIC ## SQL 
-- MAGIC <pre>
-- MAGIC APPLY CHANGES INTO LIVE.table_name
-- MAGIC FROM source
-- MAGIC KEYS (keys)
-- MAGIC [WHERE condition]
-- MAGIC [IGNORE NULL UPDATES]
-- MAGIC [APPLY AS DELETE WHEN condition]
-- MAGIC SEQUENCE BY orderByColumn
-- MAGIC [COLUMNS {columnList | * EXCEPT (exceptColumnList)}]
-- MAGIC </pre>

-- COMMAND ----------

-- MAGIC %md # Parameters
-- MAGIC * **source**
-- MAGIC * **target**
-- MAGIC * **keys**: list of column or combination of columns that uniquely identify a row in the source data. This is used to identify which CDC events apply to specific records in the target table.
-- MAGIC * **sequence_by**: The column name specifying the logical order of CDC events in the source data. Delta Live Tables uses this sequencing to handle change events that arrive out of order.
-- MAGIC * **ignore_null_updates**: Allow ingesting updates containing a subset of the target columns. When a CDC event matches an existing row and `ignore_null_updates = True`, columns with a null will retain their existing values in the target. When `ignore_null_updates = False`, existing values will be overwritten with null values.
-- MAGIC * **apply_as_deletes**: Specifies when a CDC event should be treated as a DELETE rather than an upsert. 
-- MAGIC * **column_list / except_column_list**: Subset of columns to include in the target table. `Use column_list` to specify the complete list of columns to include. Use `except_column_list` to specify the columns to exclude.

-- COMMAND ----------

-- MAGIC %md # Slow Changing Dimensions
-- MAGIC * **Type 1 SCDs** - Overwriting: In a Type 1 SCD the **new data overwrites the existing data**. Thus the existing data is lost as it is not stored anywhere else.
-- MAGIC 
-- MAGIC * **Type 2 SCDs** - Creating another dimension record: A Type 2 SCD retains the **full history of values**. When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active.
-- MAGIC 
-- MAGIC * **Type 3 SCDs** - Creating a current value field: A Type 3 SCD stores **two versions of values** for certain selected level attributes. Each record stores the previous value and the current value of the selected attribute. When the value of any of the selected attributes changes, the current value is stored as the old value and the new value becomes the current value.

-- COMMAND ----------

-- DBTITLE 1,Create dummy users table
create schema if not exists mikem_cdc_data;

create table if not exists mikem_cdc_data.users
as select
  col1 as userId,
  col2 as name,
  col3 as city,
  col4 as operation,
  col5 as sequenceNum
from (
  values
  (123, "Isabel",   "Monterrey",   "INSERT", 1),
  (124, "Raul",     "Oaxaca",      "INSERT", 1),
  (125, "Mercedes", "Tijuana",     "INSERT", 2),
  (123, null,       null,          "DELETE", 5),
  (125, "Mercedes", "Guadalajara", "UPDATE", 5),
  (123, "Isabel",   "Chihuahua",   "UPDATE", 4),
  (125, "Mercedes", "Mexicali",    "UPDATE", 4)
);

select * from mikem_cdc_data.users;

-- COMMAND ----------

-- DBTITLE 1,Example 1
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

-- MAGIC %sql
-- MAGIC create or replace temp view cdc_updates
-- MAGIC as select
-- MAGIC   col1 as userId,
-- MAGIC   col2 as name,
-- MAGIC   col3 as city,
-- MAGIC   col4 as operation,
-- MAGIC   col5 as sequenceNum
-- MAGIC from (124
-- MAGIC Raul
-- MAGIC Oaxaca
-- MAGIC   values
-- MAGIC     (126, "Armando", "Largo", "INSERT", 6),
-- MAGIC     (124, "Raul", "Whatever", "DELETE", 7)
-- MAGIC );

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dlt.apply_changes(
-- MAGIC   target = "target",
-- MAGIC   source = "cdc_updates",
-- MAGIC   keys = ["userId"],
-- MAGIC   sequence_by = col("sequenceNum"),
-- MAGIC   apply_as_deletes = expr("operation = 'DELETE'"),
-- MAGIC   except_column_list = ["operation", "sequenceNum"]
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md # SCD Type 2
-- MAGIC 
-- MAGIC `apply changes into live.cities`<br>
-- MAGIC `from stream(live.cities_updates)`<br>
-- MAGIC `keys(id)`<br>
-- MAGIC `sequence(ts)`<br>
-- MAGIC `stored as scd type 2`
-- MAGIC 
-- MAGIC Creates **__starts_at** and **__ends_at** columns

-- COMMAND ----------

-- MAGIC %md # Deletes
-- MAGIC `APPLY AS DELETE WHEN operation = "DELETE"`<br>
