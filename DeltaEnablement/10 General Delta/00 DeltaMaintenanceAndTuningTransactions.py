# Databricks notebook source
# MAGIC %md # Operations
# MAGIC 
# MAGIC ## Vacuum
# MAGIC Recursively vacuum directories associated with the Delta table and remove data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. Files are deleted according to the time they have been **logically removed from Delta’s transaction log + retention hours (specified in vacuum statement)**
# MAGIC * Runs on driver
# MAGIC * 200k - 400k files/hr
# MAGIC * Listing will happen on complete table
# MAGIC 
# MAGIC It is recommended that you set a retention interval to be at least 7 days, because old snapshots and uncommitted files can still be in use by concurrent readers or writers to the table. If VACUUM cleans up active files, concurrent readers can fail or, worse, tables can be corrupted when VACUUM deletes files that have not yet been committed. You must choose an interval that is longer than the longest running concurrent transaction and the longest period that any stream can lag behind the most recent update to the table.
# MAGIC 
# MAGIC Vacuum cleans up both files marked for deletion in the Delta Log, and it also looks for orphan files (or files considered part of a failed transaction)
# MAGIC 
# MAGIC 7 Days is safe since it's rare that a Spark job runs for > 7 days
# MAGIC 
# MAGIC Streaming: retention period > microatch interval time
# MAGIC 
# MAGIC ## Optimized Writes
# MAGIC * Runs on same cluster during/after a write
# MAGIC 
# MAGIC ## Optimize Bin-Pack
# MAGIC * Can run daily or increase frequency for read benefits (cost is resources to run it)
# MAGIC 
# MAGIC ## Optimize Z-Order
# MAGIC * Very CPU-intensive. Lot of Parquet encoding/decoding
# MAGIC * Use compute-optimized instances
# MAGIC * Use where clause
# MAGIC * minCubeSize = 100GB
# MAGIC   
# MAGIC ## Table Properties
# MAGIC * `delta.appendOnly`: Disable `UPDATE` and `DELETE` operations
# MAGIC * `delta.dataSkippingNumIndexedCols`: Number of leading column for which to collect statistics
# MAGIC * `delta.deletedFileRetentionDuration`: Default retention period for **vacuum**. Can be overriden in Vacuum statement like: `vacuum table retain 7 days`
# MAGIC * `delta.logRetentionDuration`: How long transaction log history is kept for time travel
# MAGIC * `delta.checkpointRetentionDuration`

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mikem-docs.s3.us-west-2.amazonaws.com/img/Screen+Shot+2022-02-10+at+2.36.22+PM.png"/>

# COMMAND ----------

# MAGIC %md # Delta Log Contents 
# MAGIC 
# MAGIC `/_delta_log`
# MAGIC 
# MAGIC ## .json Files
# MAGIC Files added/removed, commit info, metadata about each commit<br>
# MAGIC Created for every commit
# MAGIC 
# MAGIC ## .crc Files
# MAGIC Table info, size, num files, bytes etc ...<br>
# MAGIC Created for every commit
# MAGIC 
# MAGIC ## .checkpoint files
# MAGIC Similar to .json file <br>
# MAGIC Parquet<br>
# MAGIC Created once after 10 commits<br>
# MAGIC Summary of updates<br>
# MAGIC Used to compute table state, time travel
# MAGIC 
# MAGIC ## _last_checkpoint files
# MAGIC Indicates latest version and num records<br>
# MAGIC 
# MAGIC ### Log Configs
# MAGIC `logRetentionDuration = 7 days` = how long log files are kept. Default is 30 days <br>
# MAGIC `checkpointRetentionDuration = 1 days` = how long checkpoint files are kept. Default is 2 days 

# COMMAND ----------

# MAGIC %md # Transactions
# MAGIC 
# MAGIC ## Isolation Levels
# MAGIC 
# MAGIC ### WriteSerializable 
# MAGIC **Default** isolation level. Weaker than Serializable, but provides a good balance of consistency and availability.<br>
# MAGIC *Some* writes are allowed concurrently (not serially)<br>
# MAGIC Sequence of operations may not = sequence they are executed<br>
# MAGIC Some combinations of optimize, updates, deletes, etc can cause conflicts 
# MAGIC 
# MAGIC 
# MAGIC ### Serializable
# MAGIC **Strongest** level of isolation. All write operations happen serially.<br>
# MAGIC Always conflicts when mixing optimize, updates, deletes, etc  
# MAGIC 
# MAGIC ### Snapshot
# MAGIC Reads always read the latest committed snapshot. The **write isolation level** determines whether or not it is possible for a reader to see a snapshot of a table, that according to the history, “never existed”.
# MAGIC 
# MAGIC For the `Serializable` level, a reader always sees only tables that conform to the history. For the `WriteSerializable` level, a reader could see a table that does not exist in the Delta log. This is due to the posibility of concurrent writes in `WriteSerializable` level.
# MAGIC 
# MAGIC Note: `INSERTs` never conflict with other `INSERTS`, `OPTIMIZE` or `UPDATES/MERGES/DELETES` as they add new files
# MAGIC 
# MAGIC `ALTER TABLE t SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')`

# COMMAND ----------

# MAGIC %md # Delta Log Exceptions
# MAGIC 
# MAGIC ## FileReadException
# MAGIC * Verify id Delta table location is being used in multiple tables
# MAGIC * Refresh cache to unblock
# MAGIC * Use insert-overwrite instead
# MAGIC 
# MAGIC ## ConcurrentModificationException
# MAGIC * Happens when multiple operations happen on same partition
# MAGIC 
# MAGIC |               | **INSERT** | **UPDATES** | **OPTIMIZE** |
# MAGIC | -----------   | -------    | -------     | -------      |
# MAGIC |  **INSERT** |  No Conflict | | |
# MAGIC |  **UPDATES** | Can conflict in `Serializable`, No conflict in `WriteSerializable` | Can conflict in `Serializable` **and** `WriteSerializable` | |
# MAGIC | **OPTIMIZE** |  Cannot conflict |Can conflict in `Serializable` **and** `WriteSerializable` | Can conflict in `Serializable` **and** `WriteSerializable`|
# MAGIC 
# MAGIC `ConcurrentAppendException`: Files were added to the root of the table by a concurrent update.
# MAGIC 
# MAGIC When multiple writers are writing to the same Delta Table, you can see this exception. 
# MAGIC 
# MAGIC Cause:<br>
# MAGIC The operation you wished to perform needs a constant read-set (set of files it reads), but before your operation was over, new files were added to the table
# MAGIC 
# MAGIC Solutions:<br>
# MAGIC Partition your table, and try to operate on different partitions of your table concurrently
# MAGIC <br>Combine your concurrent operations, e.g. are you running 10 different MERGEs concurrently? Can they be done altogether?
# MAGIC <br>Schedule your writes to avoid conflicts, or just add retries to your code when you face this exception

# COMMAND ----------

# MAGIC %md # Problems and Solutions
# MAGIC 
# MAGIC ## File Not Found Exception
# MAGIC `When Querying Delta Table From a Different Cluster Caused by: java.io.FileNotFoundException: dbfs:/delta/delta-path/part-xxxx.snappy.parquet`
# MAGIC <pre>
# MAGIC Problem
# MAGIC A file referenced in the transaction log cannot be found. This occurs when data has been manually deleted from the file system rather than using the table DELETE statement.
# MAGIC 
# MAGIC Cause
# MAGIC This can occur when you delete & recreate a delta table on the same data path. Or if you delete a data file directly from the data directory using file system `rm -r command`, **instead of** using `DELETE FROM` syntax.  What happens is the data file will be removed from the data directory, however **the reference to that data file will still be there in the transaction log**. So when we query the Delta table, it tries to look for the data file that is in the transaction log. 
# MAGIC 
# MAGIC Solution
# MAGIC To temporarily resolve the issue , you can run `FSCK repair table command`. This command will repair the table, by syncing the transaction log with the actual details of the data directory. 
# MAGIC 
# MAGIC The permanent solution will be to change the process, so that you no longer delete the underlying data files of a Delta table, using filesystem commands. If your use case needs deleting/recreating Delta table, you should overwrite the Delta table instead. Overwriting Delta table is preferred over deleting/recreating it. 
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md # Table Utilities

# COMMAND ----------

# MAGIC %md ## FSCK REPAIR TABLE
# MAGIC Removes the file entries from the transaction log of a Delta table that can no longer be found in the underlying file system. This can happen when these files have been manually deleted.

# COMMAND ----------

# MAGIC %md ## Find what changed between the 2 versions of a delta table

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC use concurrency_test;
# MAGIC select * from concurr1@v4 except all (select * from concurr1@v1);

# COMMAND ----------

# MAGIC %md ## Size of table

# COMMAND ----------

sql("describe detail concurrency_test.concurr1").select("sizeInBytes").collect()

# COMMAND ----------

table("mikem.adult_data").where("education" == "Assoc-acdm")

# COMMAND ----------

table("mikem.adult_data") \
  .repartition("education") \
  .write \
  .saveAsTable(name="mikem.adult_data_part", 
               format="delta", 
               partitionBy="education", 
               path="/mnt/mikem/delta/adult-data")

# COMMAND ----------

# MAGIC %sql select * from mikem.adult_data_part where education = "Assoc-acdm"

# COMMAND ----------

# MAGIC 
# MAGIC %sql select education from mikem.adult_data where education="Bachelors"

# COMMAND ----------

# DBTITLE 1,Overwrite specific partition in Delta table from a prior version
location = "/user/hive/warehouse/mikem.db/adult_data"

spark.read.format("delta") \
  .option("versionAsOf", 0) \
  .load(location) \
  .where("partition-column = partition-value") \
  .write \
  .format("delta") \
  .mode("overwrite") \
  .option("replaceWhere", "partition-column='partition-value'") \
  .save(location)
