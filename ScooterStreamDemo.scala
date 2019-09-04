// Databricks notebook source
// MAGIC %md ## Structured Streaming Scooter Data Analysis
// MAGIC **Structured Streaming** is the **Apache Spark API** that lets you express computation on streaming data in the same way you express a batch computation on static data. The **Spark SQL** engine performs the computation incrementally and continuously updates the result as new data arrives. In doing so, **Spark** abstracts away a lot of the cumbersome details of stream processing like state management, event-time processing and late arriving data.
// MAGIC 
// MAGIC ***Concepts***
// MAGIC * Sources
// MAGIC * Sinks
// MAGIC * Microbatch - Trigger interval
// MAGIC * Event time vs processing time
// MAGIC * Late arriving data
// MAGIC * Watermarking / State (RocksDB)
// MAGIC * Continuous query
// MAGIC * Window queries
// MAGIC * Output modes

// COMMAND ----------

// MAGIC %md ## Use Case Overview
// MAGIC 
// MAGIC We will be ingesting, in real-time, a stream of data from scooters located all over the world. 
// MAGIC We will use the **Databricks Runtime** and **Structured Streaming** to anlayze and track scooters as well as send alerts when battery levels run low.
// MAGIC 
// MAGIC ***Data Structure***
// MAGIC * Device (model, id)
// MAGIC * IP address
// MAGIC * Geo location data
// MAGIC * Battery level
// MAGIC 
// MAGIC <pre>
// MAGIC Sample Payload:
// MAGIC {  
// MAGIC  "id":838146.0,
// MAGIC  "model":"sct-0003369",
// MAGIC  "latitude":38.0,
// MAGIC  "longitude":-97.0,
// MAGIC  "ip":"68.161.225.1",
// MAGIC  "cc":"USA",
// MAGIC  "cn":"United States",
// MAGIC  "battery_level":8,
// MAGIC  "timestamp":"48186-04-02T12:21:33.000Z"
// MAGIC }
// MAGIC </pre>

// COMMAND ----------

// MAGIC %md # Stream Pipeline
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/scooter-pipeline.png" width="75%"/>

// COMMAND ----------

// DBTITLE 1,Setup
// MAGIC %run ./ScooterSetup

// COMMAND ----------

// MAGIC %md ##1. Read Data

// COMMAND ----------

// DBTITLE 1,Read json stream data
val df = spark.readStream
  .option("maxFilesPerTrigger",1)
  .schema(schema)
  .json("/mnt/mikem/scooterrides/")

df.printSchema

// COMMAND ----------

// MAGIC %md ##2. Continuous Query

// COMMAND ----------

// DBTITLE 1,Continuous select
display(df.select("id", "cc", "model", "ip", "battery_level", "latitude", "longitude"))

// COMMAND ----------

// MAGIC %md ##3. Windows

// COMMAND ----------

// DBTITLE 1,Sliding Window - messages/sec
val ctWindow = df
  .select($"timestamp")
  .withWatermark("timestamp", "2 hours")
  .groupBy(window($"timestamp", "10 seconds"))
  .count()
  .orderBy("window.start")

//Display counts
val counts = ctWindow.select($"window.start".as("window start"), $"window.end".as("window end"), $"count")
display(counts)

// COMMAND ----------

// DBTITLE 1,Avg battery level over 10 minutes
val avgWindow = df.groupBy(window($"timestamp", "10 minutes")).avg("battery_level")
display(avgWindow)

// COMMAND ----------

// MAGIC %md ##4. Save to Delta

// COMMAND ----------

df.writeStream
  .format("delta")
  .outputMode("append")
//  .trigger(Trigger.ProcessingTime("10 seconds"))
  .option("checkpointLocation", "/delta/events/checkpoints/scooter_evts")
  .table("mikem.scooter_evts")

// COMMAND ----------

// MAGIC %md ##5. Analyze Data

// COMMAND ----------

// MAGIC %sql select count(*) from mikem.scooter_evts

// COMMAND ----------

// MAGIC %sql select count(*), cc from mikem.scooter_evts group by cc

// COMMAND ----------

// DBTITLE 1,Check battery levels
// MAGIC %sql select count(*), battery_level from mikem.scooter_evts group by battery_level order by battery_level

// COMMAND ----------

// MAGIC %md ##6. Send Notifications

// COMMAND ----------

// DBTITLE 1,Send notification
val ds = spark.table("mikem.scooter_evts").as[ScooterData]
val lowBatt = ds.filter(d => {d.cc == "USA" && d.battery_level < 2})
lowBatt.take(5).foreach(d=> {sendSlackNotification(s"[${d.id}]:${d.model} CHARGE SCOOTER")})

// COMMAND ----------

// DBTITLE 1,Stop streams
spark.streams.active.foreach((s: StreamingQuery) => s.stop())