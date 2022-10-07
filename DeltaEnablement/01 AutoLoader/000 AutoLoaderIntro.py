# Databricks notebook source
# MAGIC %md # Benefits 
# MAGIC 
# MAGIC * Easy
# MAGIC * Scalable
# MAGIC * Incremental (no need for bookkeeping)
# MAGIC * Based on Structured Streaming
# MAGIC * State is stored in RocksDB
# MAGIC 
# MAGIC ## Value Prop
# MAGIC * Cheap / fast listing (cost/perf) - Optimized + Incremental Listing
# MAGIC * Batching / scale
# MAGIC * Rescued Data
# MAGIC * Schema Inference (csv, json, parquet, avro, etc...)
# MAGIC * Schema Evolution (restart)
# MAGIC * File notification mode
# MAGIC * Monitoring / backlog / StreamingQueryListener
# MAGIC 
# MAGIC ## Modes
# MAGIC * DirectoryListingMode (incremental listing)
# MAGIC * FileNotificationMode
# MAGIC 
# MAGIC ## Features
# MAGIC * Identifies schema on initialization
# MAGIC * Auto-detect changes and update schema as new fields are introduced
# MAGIC * Allow type hints when schema is know for enforcement
# MAGIC * Rescue data that does not meet expectations (type-mismatch etc...)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Compare to FileStreamSource
# MAGIC In Apache Spark, you can read files incrementally using `spark.readStream.format(fileFormat).load(directory)`. Auto Loader provides the following benefits over the file source:
# MAGIC * **Scalability**: Auto Loader can discover billions of files efficiently. Backfills can be performed asynchronously to avoid wasting any compute resources.
# MAGIC * **Performance**: The cost of discovering files with Auto Loader scales with the number of files that are being ingested instead of the number of directories that the files may land in. See Optimized directory listing. Backfill (async) + new data
# MAGIC * **Schema inference and evolution support**: Auto Loader can detect schema drifts, notify you when schema changes happen, and rescue data that would have been otherwise ignored or lost.  
# MAGIC * **Cost**: Auto Loader uses native cloud APIs to get lists of files that exist in storage. In addition, Auto Loader’s file notification mode can help reduce your cloud costs further by avoiding directory listing altogether. Auto Loader can automatically set up file notification services on storage to make file discovery much cheaper.

# COMMAND ----------

# MAGIC %md # Syntax
# MAGIC 
# MAGIC `spark.readStream.format("cloudFiles")`<br>
# MAGIC `.option("cloudFiles.format", "json")`<br>
# MAGIC `.schema(schema)`<br>
# MAGIC `.load("/path")`
# MAGIC 
# MAGIC ## Options
# MAGIC * cloudFiles.format
# MAGIC   * avro
# MAGIC   * csv 
# MAGIC   * json
# MAGIC   * orc
# MAGIC   * parquet 
# MAGIC   * text 
# MAGIC * cloudFiles.schemaHints
# MAGIC * cloudFiles.schemaLocation
# MAGIC * cloudFiles.includeExistingFiles 
# MAGIC * cloudFiles.maxFilesPerTrigger
# MAGIC * cloudFiles.useNotifications
# MAGIC * cloudFiles.connectionString
# MAGIC * cloudFiles.fetchParallelism
# MAGIC * cloudFiles.region 
# MAGIC * cloudFiles.resourceGroup 
# MAGIC * cloudFiles.tenantId 
# MAGIC * cloudFiles.subscriptionId 
# MAGIC * cloudFiles.clientId
# MAGIC * cloudFiles.clientSecret
# MAGIC 
# MAGIC ## Batch Mode
# MAGIC `Trigger.AvailableNow` tells Auto Loader to process all files that arrived before the query start time. New files that are uploaded after the stream has started will be ignored until the next trigger.
# MAGIC 
# MAGIC 
# MAGIC ## Event retention
# MAGIC Auto Loader keeps track of discovered files in the **checkpoint location using RocksDB** to provide exactly-once ingestion guarantees. For high volume datasets, you can use the `cloudFiles.maxFileAge` option to expire events from the checkpoint location to reduce your storage costs and Auto Loader start up time. The **minimum value** that you can set for `cloudFiles.maxFileAge = "14 days"`. Deletes in RocksDB appear as tombstone entries, therefore you should expect the storage usage to increase temporarily as events expire before it starts to level off.
# MAGIC 
# MAGIC ⚠️ Becareful using this as files could be ignored

# COMMAND ----------

# MAGIC %md # Schema Inference
# MAGIC 
# MAGIC JSON, CSV, Parquet, Avro (Others???)
# MAGIC 
# MAGIC Happens at stream startup.
# MAGIC 
# MAGIC To infer the schema, Auto Loader samples the first 50 GB or 1000 files that it discovers, whichever limit is crossed first. To avoid incurring this inference cost at every stream start up, and to be able to provide a stable schema across stream restarts, you must set the option `cloudFiles.schemaLocation`
# MAGIC 
# MAGIC By default, Auto Loader infers columns in text-based file formats like CSV and JSON as string columns. Inferring the data as string can help avoid schema evolution issues such as numeric type mismatches (integers, longs, floats). If you want to retain the original Spark schema inference behavior, set the option `cloudFiles.inferColumnTypes = true`.
# MAGIC 
# MAGIC When Auto Loader infers the schema, a **rescued data** column is automatically added to your schema as `_rescued_data`. See the section on rescued data column and schema evolution for details.
# MAGIC 
# MAGIC Auto Loader also infers partition columns by examining the source directory structure and looks for file paths that contain the `/key=value/` structure
# MAGIC 
# MAGIC If the source directory is empty, Auto Loader requires you to provide a schema as there is no data to perform inference.
# MAGIC 
# MAGIC You should expect schema inference to take **a couple of minutes** for very large source directories during initial schema inference. You shouldn’t observe significant performance hits otherwise during stream execution.

# COMMAND ----------

# MAGIC %md # Schema Evolution
# MAGIC Auto Loader detects the addition of new columns as it processes your data. By default, addition of a new column will cause your streams to stop with an **UnknownFieldException**. Before your stream throws this error, Auto Loader performs schema inference on the latest micro-batch of data, and updates the schema location with the latest schema. 
# MAGIC 
# MAGIC New columns are merged to the end of the schema. The data types of existing columns remain unchanged. By setting your Auto Loader stream within a Databricks job, you can get your stream to restart automatically after such schema changes.
# MAGIC 
# MAGIC Auto Loader supports the following modes for schema evolution, which you set in the option `cloudFiles.schemaEvolutionMode`:
# MAGIC * **addNewColumns**: Default mode when a schema is not provided to Auto Loader. The streaming job will fail with an UnknownFieldException. New columns are added to the schema. Existing columns do not evolve data types. addNewColumns is not allowed when the schema of the stream is provided. You can instead provide your schema as a schema hint instead if you want to use this mode. The stream has to be failed, and re-run. It is a limitation of Spark SQL. It's not possible to change the schema of a DataFrame dynamically. You need to get a new reference - therefore the commands need to be re-run (which can only be achieved by failing the stream). DLT auto-restarts pipelines very efficiently by the way
# MAGIC 
# MAGIC * **failOnNewColumns**: If Auto Loader detects a new column, the stream will fail. It will **not restart** unless the provided schema is updated, or the offending data file is removed.
# MAGIC 
# MAGIC * **rescue**: The stream runs with the very first inferred or provided schema. Any data type changes or new columns that are added are rescued in the rescued data column that is automatically added to your stream’s schema as _rescued_data. In this mode, your stream will not fail due to schema changes.
# MAGIC 
# MAGIC * **none**: The default mode when a schema is provided. Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescued data column is provided separately as an option.
# MAGIC 
# MAGIC Partition columns are not considered for schema evolution. If you had an initial directory structure like `base_path/event=click/date=2021-04-01/f0.json`, and then start receiving new files as `base_path/event=click/date=2021-04-01/hour=01/f1.json`, the hour column is ignored. To capture information for new partition columns, set `cloudFiles.partitionColumns` to event,date,hour.

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimizations
# MAGIC Auto Loader optimizations provide performance improvements and cost savings when listing nested directories in cloud storage.
# MAGIC 
# MAGIC ## Optimized File Listing
# MAGIC API change to reduce number of list API calls
# MAGIC For example, if you had files being uploaded as `/XXX/YYYY/MM/DD/HH/fileName`, to find all the files in these directories, Auto Loader used to do a parallel listing of all subdirectories, causing 365 (per day) * 24 (per hour) = 8760 LIST API directory calls to the underlying storage for each year directory. By receiving a flattened response from these storage systems, Auto Loader reduces the number of API calls **to the number of files in the storage system divided by the number of results returned by each API call** (1000 for S3, 5000 for ADLS Gen2, and 1024 for GCS), greatly reducing your cloud costs.
# MAGIC 
# MAGIC ## Incremental Listing Mode
# MAGIC Lists **from the last filename processed** so as to only capture the new files. 
# MAGIC 
# MAGIC With incremental listing (which will be default mode in autolaoder) you get simplicity of directory listing with performance of notification listing mode
# MAGIC 
# MAGIC **Cons**:
# MAGIC Doesn't work for all file naming conventions. This mode will be effective for data uploaded by:
# MAGIC * AWS DMS
# MAGIC * Azure Data Factory
# MAGIC * Kinesis Firehose
# MAGIC * Anything else that uploads files like `yyyy/mm/dd/timestamp-uuid`
# MAGIC 
# MAGIC if users set `useIncrementalListing = true`, they can set `backfillInterval` to make sure auto loader captures everything in case of any out-of-order files. For example, due to some cloud glitches, a file is arriving late, etc

# COMMAND ----------

# MAGIC %md # Architecture
# MAGIC 
# MAGIC As files are discovered, their metadata is persisted in a scalable key-value store (RocksDB) in the checkpoint location of your Auto Loader pipeline. This key-value store ensures that data is processed exactly once. You can switch file discovery modes across stream restarts and still obtain exactly-once data processing guarantees. In fact, this is how Auto Loader can both perform a backfill on a directory containing existing files and concurrently process new files that are being discovered through file notifications.
# MAGIC 
# MAGIC In case of failures, Auto Loader can resume from where it left off by information stored in the checkpoint location and continue to provide exactly-once guarantees when writing data into Delta Lake. You don’t need to maintain or manage any state yourself to achieve fault tolerance or exactly-once semantics.

# COMMAND ----------

# MAGIC %md # Cluster Sizing 
# MAGIC 
# MAGIC Set `maxfilesPerTrigger` to be a fraction of cores. 64 cores = ie `maxFilesPerTrigger = num cores x n` Factor should vary based on file size

# COMMAND ----------

# MAGIC %md # Configs
# MAGIC ## Message Fetcher Options
# MAGIC * `queueFetchInterval`:  Default is 5s.  How frequently to poll the queueing service in the absence of new messages. When we have  messages, we will poll continuously. When we run out of messages, we will back off according to this interval to reduce costs. It can’t be longer than 20 seconds.
# MAGIC * `maxMessagesPerFetch`: Default is 10. How many notifications that can be returned by the queue service in a single fetch. It can’t be more than 10.
# MAGIC * `deletionErrorRetries`: Default is 4. Number of times to retry the deletion of a batch of messages during remote server errors.
# MAGIC * `deletionParallelism`: Default is 8.  Number of threads to use when deleting messages in the queueing service.
# MAGIC * `fetchParallelism`: Default is 1. Number of threads to use when fetching messages from the queueing service.
# MAGIC * `deletionBatchSize`: Default is  80. Each batch will be divided across deletionParallelism threads.
# MAGIC * `visibilityTimeout`: Default is 300 seconds. The duration (in seconds) that the fetched messages are hidden from subsequent requests after  being retrieved.
# MAGIC 
# MAGIC ## File Notification Source Options
# MAGIC * `maxWriteBufferSize`: Default is 10000. How many notifications to buffer before flushing.
# MAGIC * `maxFileAge`: Default is 7 days.  The total amount of time to keep a file in the transaction log as well as the deduplication map. This needs to be at least as long as the queue retention period to prevent duplicates.
# MAGIC * `fileExpirationPeriod`: Default is 1 hour. How often to age out files from the transaction log and the deduplication map.
# MAGIC * `fileCleanupBufferSize`: Default is maxWriteBufferSize. Max files to delete in a single commit when deleting files from the transaction log.
# MAGIC * `alowOverwrites`: Default is false. Whether to allow multiple messages of the same type to be committed to the transaction log multiple times, which would allow reprocessing of the same file in cases where it gets overwritten.
# MAGIC * `triggerOnceQueueFlushTimeout`:  Default is 60s. After the timeout, the queue flushing completion flag is set to true.
# MAGIC * `partitionColumns`: partition columns string. It needs to be a comma-separated list of distinct column names.
# MAGIC * `pathRewrites`:  An optional mapping of fully qualified bucket/container-key prefixes to fully qualified dbfs paths to handle mount points. It needs to be a json representation of a string to string map.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC [Source](https://docs.google.com/document/d/1RcDB3lEQUZhBPSgk8jqpS4mBzoFeh_jt01J6QzylPSg/edit#)

# COMMAND ----------

# MAGIC %md # Backfills

# COMMAND ----------

# MAGIC %md # File Notification Mode

# COMMAND ----------

# MAGIC %md # Objects
# MAGIC 
# MAGIC * FileEvent
# MAGIC * FileEventAcceptor
# MAGIC * MessageFetcher
# MAGIC * 
# MAGIC * CloudFilesSourceMetadataLog

# COMMAND ----------

# MAGIC %md # Reference
# MAGIC 
# MAGIC * [Autoloader Wiki](https://databricks.atlassian.net/wiki/spaces/UN/pages/2620227652/Autoloader)
# MAGIC * [Autoloader Under the Hood](https://docs.google.com/document/d/17ncbW1VM3nhEQ4kHud_a4_4vilieix8CY9H9mH5eg0A)
# MAGIC * [Autoloader Eng Office Hours](https://drive.google.com/file/d/1DY1ALC1cqiom9y4-DYEHd9IVp_sorqx3)
