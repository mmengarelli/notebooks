-- Databricks notebook source
-- MAGIC %md # Restoring Delta Tables
-- MAGIC 
-- MAGIC You can restore a Delta table to its earlier state by using the RESTORE command. A Delta table internally maintains historic versions of the table that enable it to be restored to an earlier state. A version corresponding to the earlier state or a timestamp of when the earlier state was created are supported as options by the **RESTORE** command.
-- MAGIC 
-- MAGIC Note: Restoring a table to an older version where the data files were deleted manually or by vacuum will fail. Restoring to this version partially is still possible if `spark.sql.files.ignoreMissingFiles = true`.
-- MAGIC 
-- MAGIC ## Restore Metrics
-- MAGIC 
-- MAGIC **RESTORE** reports the following metrics as a single row DataFrame once the operation is complete:
-- MAGIC * `table_size_after_restore` The size of the table after restoring.
-- MAGIC * `num_of_files_after_restore` The number of files in the table after restoring.
-- MAGIC * `num_removed_files` Number of files removed (logically deleted) from the table.
-- MAGIC * `num_restored_files` Number of files restored due to rolling back.
-- MAGIC * `removed_files_size` Total size in bytes of the files that are removed from the table.
-- MAGIC * `restored_files_size` Total size in bytes of the files that are restored.

-- COMMAND ----------

drop database if exists delta_utils cascade;
create database delta_utils;
use delta_utils;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import dbldatagen as dg
-- MAGIC from pyspark.sql.types import IntegerType, FloatType, StringType
-- MAGIC 
-- MAGIC spec = dg.DataGenerator(spark, name="test_data_set1", rows=100, partitions=3) \
-- MAGIC   .withIdOutput() \
-- MAGIC   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=5)
-- MAGIC 
-- MAGIC df = spec.build()
-- MAGIC df.write.saveAsTable("delta_utils.restore_me")
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

delete from restore_me where id in (0,1,2);
update restore_me set r_0 = 99999 where id = 67;
insert into restore_me values (101, 1, 2, 3, 4, 5);
describe history restore_me;

-- COMMAND ----------

restore table restore_me to version as of 2

-- COMMAND ----------

restore table restore_me to timestamp as of '2022-02-10T15:55:38'

-- COMMAND ----------

describe history restore_me
