-- Databricks notebook source
-- MAGIC %fs ls /mnt/

-- COMMAND ----------

-- MAGIC %md # Cloning Tables
-- MAGIC Clone - point in time clone 
-- MAGIC All metadata is duplicated (stats, partition info, schema, constraints etc...)
-- MAGIC Clones have separate lineage. Changes to a cloned table do not affect the sources
-- MAGIC Changes to the source are not reflected in the clone.
-- MAGIC Can be written to a user-defined location
-- MAGIC 
-- MAGIC Cloning is not the same as CTAS. A clone copies the metadata of the source table in addition to the data.<br>
-- MAGIC A cloned table has an independent history from its source table. Time travel queries on a cloned table will not work with the same inputs as they work on its source table.<br>
-- MAGIC Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.
-- MAGIC 
-- MAGIC ## Clone Types
-- MAGIC ### Shallow Clone
-- MAGIC * Zero copy clone - metadata only
-- MAGIC * Best for short-lived activities like qa testing as modifications so the source can affect the clone
-- MAGIC * Since queries actually hit the source table, modifications to the source can affect the clone
-- MAGIC * New data files are written to the clone's storage location
-- MAGIC * Changes to the source table mark files as no longer available 
-- MAGIC * Vacuuming source table permanently removes data (clone can no longer access)
-- MAGIC * References to that source data will cause queries to fail 
-- MAGIC 
-- MAGIC ### Deep Clone
-- MAGIC * A clone that copies the source table data and the metadata of the existing table.
-- MAGIC * Alternate to CTAS
-- MAGIC * Data and metadata is copied
-- MAGIC * Keeps clone and source in sync - good for archive or DR scenarios
-- MAGIC * Copy is optimized transaction and robust
-- MAGIC * Data is copied incrementally as data is added or modified
-- MAGIC * After cloning the files will be in sync between both tables
-- MAGIC * Clone has separate versions/history - time travel will not be the same between the 2 tables
-- MAGIC 
-- MAGIC #### Incremental Clone
-- MAGIC * Syntax for incremental clone: `create or replace table t_clone deep clone t`
-- MAGIC * Only newly written data files are copied
-- MAGIC * Updates, deletes, and appends are automatically applied
-- MAGIC * Data files will be identical in both tables after cloning
-- MAGIC 
-- MAGIC ## Metrics
-- MAGIC CLONE reports the following metrics as a single row DataFrame once the operation is complete:
-- MAGIC * source_table_size: Size of the source table that’s being cloned in bytes.
-- MAGIC * source_num_of_files: The number of files in the source table.
-- MAGIC * num_removed_files: If the table is being replaced, how many files are removed from the current table.
-- MAGIC * num_copied_files: Number of files that were copied from the source (0 for shallow clones).
-- MAGIC * removed_files_size: Size in bytes of the files that are being removed from the current table.
-- MAGIC * copied_files_size: Size in bytes of the files copied to the table.

-- COMMAND ----------

-- MAGIC %md # Use Cases
-- MAGIC * Data archiving
-- MAGIC * Machine learning flow reproduction
-- MAGIC * Short-term experiments on a production table
-- MAGIC * Data sharing
-- MAGIC * Table property overrides

-- COMMAND ----------

-- MAGIC %md # Examples
-- MAGIC 
-- MAGIC * CREATE TABLE delta.`/data/target/` CLONE delta.`/data/source/` -- Create a deep clone of /data/source at /data/target
-- MAGIC * CREATE OR REPLACE TABLE db.target_table CLONE db.source_table -- Replace the target
-- MAGIC * CREATE TABLE IF NOT EXISTS TABLE delta.`/data/target/` CLONE db.source_table -- No-op if the target table exists
-- MAGIC * CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source`
-- MAGIC * CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` VERSION AS OF version
-- MAGIC * CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` TIMESTAMP AS OF timestamp_expression -- timestamp can be like “2019-01-01” or like date_sub(current_date(), 1)

-- COMMAND ----------

drop database if exists delta_utils cascade;
create database delta_utils;

-- COMMAND ----------

-- DBTITLE 1,Generate dummy data 
-- MAGIC %python
-- MAGIC import dbldatagen as dg
-- MAGIC from pyspark.sql.types import IntegerType, FloatType, StringType
-- MAGIC 
-- MAGIC spec = dg.DataGenerator(spark, name="test_data_set1", rows=100, partitions=4) \
-- MAGIC   .withIdOutput() \
-- MAGIC   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)", numColumns=5)
-- MAGIC 
-- MAGIC df = spec.build()
-- MAGIC df.write.saveAsTable("delta_utils.delta_1")
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md # Shallow clone

-- COMMAND ----------

create or replace table delta_utils.shallow_clone_1 shallow clone delta_utils.delta_1

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/delta_utils.db/shallow_clone_1/

-- COMMAND ----------

-- MAGIC %md # Deep clone

-- COMMAND ----------

create or replace table delta_utils.deep_clone_1 deep clone delta_utils.delta_1

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/delta_utils.db/deep_clone_1/

-- COMMAND ----------

-- MAGIC %md # Incremental clone

-- COMMAND ----------

-- current count in clone
select count(*) from delta_utils.deep_clone_1

-- COMMAND ----------

-- add some results to source 
insert into delta_utils.delta_1 values(101,1,2,3,4,5);
insert into delta_utils.delta_1 values(102,1,2,3,4,5);

-- incremental clone
create or replace table delta_utils.deep_clone_1 deep clone delta_utils.delta_1;

-- check count now
select count(*) from delta_utils.deep_clone_1;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC checkTableVersions()
