# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables
# MAGIC Framework for building reliable, maintainable, and testable data processing pipelines.<br>
# MAGIC Can create multi-notebook pipelines that mix Sql and Python.<br>Supports Repos and Photon.<br>
# MAGIC A live table is a materialized view. A table defined by SQL.
# MAGIC 
# MAGIC ## Value Prop
# MAGIC * **Agility**: Easily manage large and complex ETL pipelines, seamlessly
# MAGIC * **Reliability**: Ensure data quality, error recovery (expectations, qurantine)
# MAGIC * **Scale**: Meet SLA's without the need to manage jobs or clusters
# MAGIC * **Observability**: (user interface, monitoring, event log, audit log, lineage, performance, data quality) 
# MAGIC 
# MAGIC ## Complete Live Tables (batch)
# MAGIC For **batch** mode and loads the data set as specified. Loads entire data set as from query or storage location. Create or replace.<br>
# MAGIC `create live table` is simialr to `create or replace view` but with all the benefits of DLT. It reprocesses all the data with each run.
# MAGIC 
# MAGIC ## Streaming Live Tables (stream)
# MAGIC For data **streams**, it can process only newly arrived data since the last update. Keeps a checkpoint/watermark. Appends new records.<br>
# MAGIC Computes results from append-only sources (Kafka, Kinesis, Auto Loader etc.)<br>
# MAGIC * Exactly-once
# MAGIC * Configurable retention
# MAGIC * Watermark: How long do we remember a row? In case late arriving data comes and we need to update it
# MAGIC * Does not reprocess data (cannot handle upstream table changes updates/deletes)
# MAGIC * You do not need to provide a schema or checkpoint location because Delta Live Tables automatically manages these settings for your pipelines.
# MAGIC 
# MAGIC ### handling upstream changes
# MAGIC * `ignoreDeletes`: ignore transactions that delete data at partition boundaries.
# MAGIC * `ignoreChanges`: re-process updates if files had to be rewritten in the source table due to a data changing operation such as UPDATE, MERGE INTO, DELETE (within partitions), or OVERWRITE. Unchanged rows may still be emitted, therefore your downstream consumers should be able to handle duplicates. Deletes are not propagated downstream. ignoreChanges subsumes ignoreDeletes. Therefore if you use ignoreChanges, your stream will not be disrupted by either deletions or updates to the source table.
# MAGIC 
# MAGIC ## Triggered 
# MAGIC Triggered pipelines (default) update each table with whatever data is currently available and then **stop the cluster** running the pipeline. Triggered pipelines can reduce resource consumption and expense since the cluster runs only long enough to execute the pipeline
# MAGIC 
# MAGIC ## Continuous 
# MAGIC Continuous pipelines update tables continuously as input data changes. Once an update is started, it continues to run until manually stopped. Continuous pipelines require an **always-running cluster** but ensure that downstream consumers have the most up-to-date data.
# MAGIC 
# MAGIC ## Live Dependencies
# MAGIC * For dependencies from the same pipeline - Available to current pipeline (cluster)
# MAGIC * Read from the `LIVE` schema (Not in catalog)
# MAGIC * Registered in metadata as query not physical table
# MAGIC 
# MAGIC ## Development
# MAGIC * Reuses a cluster to avoid restarts
# MAGIC * Disables retries so you can immediately detect and fix errors
# MAGIC 
# MAGIC ## Production
# MAGIC * Restarts the cluster for specific recoverable errors, including memory leaks and stale credentials
# MAGIC * Retries execution in the event of specific errors, for example, a failure to start a cluster
# MAGIC 
# MAGIC ## Enhanced Autoscaling
# MAGIC Adds to the existing cluster autoscaling functionality with the following features:
# MAGIC * Implements optimization of streaming workloads
# MAGIC * Adds enhancements to improve the performance of batch workloads<br>
# MAGIC * Proactively shuts down under-utilized nodes while guaranteeing there are no failed tasks during shutdown<br>
# MAGIC These optimizations result in more efficient cluster utilization, reduced resource usage, and lower cost
# MAGIC 
# MAGIC ## Editions
# MAGIC * Core
# MAGIC * Pro 
# MAGIC * Advanced
# MAGIC 
# MAGIC ## Limitations
# MAGIC * DLT views are only available to other tables in the pipeline
# MAGIC * Delta Live Tables can only be used to update Delta tables
# MAGIC * Only a single source can write to a table. You can use `UNION` If you need to combine multiple inputs to create a table
# MAGIC <br><br>
# MAGIC 
# MAGIC ## DLT Event Log
# MAGIC The event log is a log of all the events that happen with the pipeline (e.g. A user started an update, A dataset is defined during the update run, a flow completes). The control plane’s version of the event log is used to power the UI. This version is also shown in the UI at the bottom of a pipeline’s details page.
# MAGIC 
# MAGIC Up-to-date documentation on Delta Live Tables can be found [here](https://docs.databricks.com/data-engineering/delta-live-tables/index.html)
# MAGIC 
# MAGIC [DLT Cookbook](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-cookbook.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Sources
# MAGIC 
# MAGIC You can use the following external data sources to create datasets:
# MAGIC * Any data source that Databricks Runtime directly supports.
# MAGIC * Any file in cloud storage or DBFS
# MAGIC * Streams
# MAGIC * DLT table(s)
# MAGIC * Stream-batch joins
# MAGIC 
# MAGIC Databricks recommends using **Auto Loader** for pipelines that read data from supported file formats.

# COMMAND ----------

# MAGIC %md # Syntax
# MAGIC ## SQL
# MAGIC AutoLoader source: `create live table bronze as select * from cloud_files('/data', 'json')`<br>
# MAGIC Table source: `create live table bronze as select * from db.raw`<br>
# MAGIC View: `create live view bronze as select * from db.raw`<br>
# MAGIC Streaming live table: `create streaming live table streaming_bronze as select * from cloud_files('/data', 'json')`<br>
# MAGIC With constraints: <br>
# MAGIC 
# MAGIC ## Python
# MAGIC Apply the `@dlt.view` or `@dlt.table` decorator to a function to define a view or table in Python. You can use the **function name** or the **name parameter** to assign the table or view name. 
# MAGIC 
# MAGIC `dlt.read()`<br>
# MAGIC `spark.table()`<br>
# MAGIC `dlt.read_stream()`
# MAGIC 
# MAGIC AutoLoader source:<br>
# MAGIC `@dlt.table(name="bronze")`<br>
# MAGIC `def bronze():`<br>
# MAGIC `return (spark.readStream`<br>
# MAGIC `.format("cloudFiles")`<br>
# MAGIC `.option("cloudFiles.format", "json")`<br>
# MAGIC `.load("/data"))`
# MAGIC 
# MAGIC Table source:<br>
# MAGIC `@dlt.table`<br>
# MAGIC `def bronze():`<br>
# MAGIC   `return spark.table("db.raw")`
# MAGIC   
# MAGIC DLT source:<br>
# MAGIC `@dlt.table`<br>
# MAGIC `def customers_filtered():`<br>
# MAGIC `return dlt.read("customers_raw").where(...)`
# MAGIC 
# MAGIC Stream source:<br>
# MAGIC `@dlt.table`<br>
# MAGIC `def silver():`<br>
# MAGIC `return dlt.read_stream("stream")`
# MAGIC 
# MAGIC LIVE Keyword<br>
# MAGIC Refer to live tables in native Spark code 
# MAGIC 
# MAGIC Partition Columns
# MAGIC `partition_cols=["col1", "col2"]`
# MAGIC 
# MAGIC Table Properties
# MAGIC `table_properties={"key" : "val"}`
# MAGIC 
# MAGIC 
# MAGIC Parameterization
# MAGIC <pre>"configuration": {
# MAGIC   "mypipeline.startDate": "2021-01-02"
# MAGIC }
# MAGIC start_date = spark.conf.get("mypipeline.startDate")
# MAGIC </pre>

# COMMAND ----------

# MAGIC %md # Data Quality (Expectations)
# MAGIC 
# MAGIC ## Expectations
# MAGIC * Keeps bad data in record: `expect`
# MAGIC * Fails pipeline: `expect_or_fail`
# MAGIC * Drops record: `expect_or_drop`
# MAGIC * Expect multiple: `@dlt.expect_all_or_drop(...)`
# MAGIC 
# MAGIC You can view data quality metrics such as the number of records that violate an expectation by querying the Delta Live Tables event log.
# MAGIC 
# MAGIC `valid_pages = {"valid_count": "count > 0", "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"}`<br><br>
# MAGIC 
# MAGIC `@dlt.table`<br>
# MAGIC `@dlt.expect_all(valid_pages)`<br>
# MAGIC `def data():`<br>
# MAGIC `  ...`
# MAGIC 
# MAGIC ### SQL Expectations
# MAGIC Using the constraint keyword
# MAGIC <pre>
# MAGIC CREATE STREAMING LIVE TABLE recordings_enriched
# MAGIC   (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
# MAGIC AS SELECT device_id, a.mrn, name, time, heartrate
# MAGIC   FROM STREAM(live.recordings_parsed) a 
# MAGIC </pre>
# MAGIC 
# MAGIC [More on data quality constraints](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html)
# MAGIC 
# MAGIC ## Quarrantine
# MAGIC Create rules that are the inverse of the expectations you’ve defined and use those rules to save the invalid records to a separate table. 
# MAGIC 
# MAGIC [More on quarrantine](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-cookbook.html#quarantine-invalid-data)

# COMMAND ----------

# MAGIC %md-sandbox # DML / CRUD
# MAGIC You can run DML operations on live tables (outside of DLT pipelines)<br>
# MAGIC DML works on **streaming live tables only!!**<br>
# MAGIC Streaming live tables are stateful (they will remember changes made)<br>
# MAGIC DML on live tables is undone by the next update.<br>
# MAGIC It does not make sense to run DML on a batch live table.
# MAGIC 
# MAGIC Examples:
# MAGIC * GDPR delete
# MAGIC * Retention delete
# MAGIC * Scrub PII
# MAGIC 
# MAGIC ## Example GDPR Reference Archiecture
# MAGIC <img src="https://mikem-docs.s3.us-west-2.amazonaws.com/img/gdpr_ref_arch.png" style="height: 300px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>

# COMMAND ----------

# MAGIC %md # Managing Pipelines
# MAGIC 
# MAGIC You can create, run, manage, and monitor a Delta Live Tables pipeline using the **UI or the Delta Live Tables API**. You can also run your pipeline with an orchestration tool such as Databricks jobs.
# MAGIC 
# MAGIC By default, Delta Live Tables **automatically scales your pipeline clusters to optimize performance and cost**. 
# MAGIC Databricks recommends cluster autoscaling, but **you can optionally disable autoscaling** and configure a fixed number of worker nodes for your pipeline clusters when you create or edit a pipeline.
# MAGIC 
# MAGIC Delta Live Tables performs maintenance tasks on tables every 24 hours. Maintenance can improve query performance and reduce cost by removing old versions of tables. By default, the system performs a full `OPTIMIZE` operation followed by `VACUUM`. You can disable OPTIMIZE for a table by setting `pipelines.autoOptimize.managed = false` in the table properties for the table.

# COMMAND ----------

# MAGIC %md # Automated Data Management
# MAGIC 
# MAGIC 
# MAGIC |  Best Practices | Physical Date  | Schema Evolution  |
# MAGIC |---|---|---|
# MAGIC | optimizeWrite, autoCompact, tuneFileSizesForRewrites | runs vacuum  daily, runs optimize daily | Schema evolution is handled for you. Modifying a live table transformation to add/remove/rename a column will automatically do the right thing. When removing a column in a streaming live table, old values are preserved  |

# COMMAND ----------

# MAGIC %md # Target (Publishing Data) 
# MAGIC 
# MAGIC You can make tables available to external users/applications by publishing it to the metastore. You can do this with the `target` config specifying the target database.
# MAGIC 
# MAGIC `"target":"db.table"`

# COMMAND ----------

# MAGIC %md # Orchestration
# MAGIC 
# MAGIC * Databricks Jobs `task = Pipeline`
# MAGIC * Airfllow `pipeline_task={"pipeline_id": "123"}`
# MAGIC * ADF `https://instance/api/2.0/pipelines/123/updates`
# MAGIC * API `curl --netrc --request POST https://host/api/2.0/pipelines --data @pipeline-settings.json`

# COMMAND ----------

# MAGIC %md # Table Properties
# MAGIC * `pipelines.autoOptimize.managed = true` Disable automatic optimize. Vacuum cannot be turned off. 
# MAGIC * `pipelines.autoOptimize.zOrderCols = col1, col2`
# MAGIC * `pipelines.reset.allowed = true` - Controls whether a full-refresh is allowed for this table
# MAGIC * `pipelines.trigger.interval`
# MAGIC   * Four seconds for streaming queries
# MAGIC   * One minute for complete queries
# MAGIC   * Ten minutes for complete queries when some data sources may be non-Delta
# MAGIC   * <pre>{"pipelines.trigger.interval" : "1 hour"}
# MAGIC   {"pipelines.trigger.interval" : "10 seconds"}</pre>

# COMMAND ----------

# MAGIC %md # Tables vs Views
# MAGIC <table>
# MAGIC   <tr>
# MAGIC     <td>Tables</td>
# MAGIC     <td>Views</td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>
# MAGIC A table is a <b>materialized</b> copy of the results of a query
# MAGIC Creating and updating a tables costs <b>storage and compute</b>
# MAGIC The results can be reused by multiple downstream computations
# MAGIC Views cannot be queries and are only available to other tables in the pipeline.
# MAGIC     </td>
# MAGIC     <td>
# MAGIC     Views are <b>virtual</b> a name for a query
# MAGIC Use views to break up large/complex queries
# MAGIC Expectations on views validate correctness of intermediate results
# MAGIC Views are recomputed every time they are queried
# MAGIC     </td>
# MAGIC   </tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md # Full Table Refresh
# MAGIC 
# MAGIC Clear state using full refresh. Clears checkpoints, state, table data and reprocesses all data.<br>
# MAGIC Perform backfills after critical changes<br>
# MAGIC Refreshes all streaming tables in the pipeline<br>
# MAGIC Note: partial refresh is being developed

# COMMAND ----------

# MAGIC %md # Failure Handling
# MAGIC 
# MAGIC DLT Automates Failure Recovery<br>
# MAGIC Transient issues are handled by built-in retry logic<br>
# MAGIC DLT uses escalating retries to balance speed with reliability
# MAGIC * Retry the individual transaction
# MAGIC * Next, restart the cluster in case its in a bad state
# MAGIC * If DBR was upgraded since the last success, automatically try the old version to detect regressions.
# MAGIC 	* If a regression was detected, automatically open a DB support ticket
# MAGIC 
# MAGIC 
# MAGIC Transactionally
# MAGIC * Operations on tables are atomic.
# MAGIC * Updates to different tables in a pipeline are not atomic. Best effort to update as many as possible.

# COMMAND ----------

# MAGIC %md-sandbox # Observability
# MAGIC 
# MAGIC <img src="https://mikem-docs.s3.us-west-2.amazonaws.com/img/Observability.png" style="height: 200px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
# MAGIC 
# MAGIC <img src="https://mikem-docs.s3.us-west-2.amazonaws.com/img/EventLog2.png" style="height: 200px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
# MAGIC 
# MAGIC ## Pipeline UI
# MAGIC * Visualize data flows between tables
# MAGIC * Discover metadata and quality of each table
# MAGIC * Access to historical updates
# MAGIC Control operations
# MAGIC Dive deep into events
# MAGIC 
# MAGIC ## Event Log
# MAGIC The event log is stored in `../system/events` under the storage location. 
# MAGIC For example, if you have configured your pipeline storage setting as `/data`, the event log is stored in the `/data/system/events` path in DBFS.
# MAGIC 
# MAGIC `df = spark.read.format('delta').load("/data/system/events")`
# MAGIC 
# MAGIC ## Metrics
# MAGIC | Operational Statistics | Provenance | Data Quality | 
# MAGIC |---|---|---|
# MAGIC |Time and current status, for all operations | Table schemas, definitions, and declared properties  |  Expectation pass / failure / drop statistics |
# MAGIC | Pipeline and cluster configurations  | Table-level lineage  |  Input/Output rows that caused expectation failures |
# MAGIC |  Row counts |  Query plans used to update tables |   |
# MAGIC 
# MAGIC 
# MAGIC [Notebook Example](https://adb-2541733722036151.11.azuredatabricks.net/?o=2541733722036151#notebook/3644013966137968)

# COMMAND ----------

# MAGIC %md # CDC
# MAGIC See this [notebook](https://adb-2541733722036151.11.azuredatabricks.net/?o=2541733722036151#notebook/4002663362004968)

# COMMAND ----------

# MAGIC %md # Advanced
# MAGIC 
# MAGIC ## Streaming Aggregations
# MAGIC One level only
# MAGIC 
# MAGIC ## Stream-Stream Joins
# MAGIC To join data from 2 streams that arrives simultaneously (or within a similar time window)<br>
# MAGIC Ex: Ad tech (impression + click) 

# COMMAND ----------

# MAGIC %md # Other
# MAGIC 
# MAGIC ## Trigger vs Continuous 
# MAGIC * A table being complete (batch) vs streaming is orthogonal to whether a pipeline is run as triggered or continuous
# MAGIC * A triggered pipeline, we'll update all the tables once and shut down the cluster
