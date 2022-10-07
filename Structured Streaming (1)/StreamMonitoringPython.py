# Databricks notebook source
# DBTITLE 1,Setup
# MAGIC %sql
# MAGIC drop table if exists mikem.stream_log;
# MAGIC 
# MAGIC create table if not exists mikem.stream_log (
# MAGIC   time long,
# MAGIC   operation string
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/mnt/mikem/checkpoints/dummy3

# COMMAND ----------

schema = spark.read.json("/mnt/training/retail-org/solutions/sales_stream/12-05.json").schema

df = spark.readStream.schema(schema) \
  .format("json") \
  .option("path", "/mnt/training/retail-org/solutions/sales_stream") \
  .option("maxFilesPerTrigger", 1) \
  .load()

# COMMAND ----------

df.writeStream \
  .format("console") \
  .trigger(processingTime='300 seconds') \
  .queryName("stream_1") \
  .option("checkpointLocation", "dbfs:/mnt/mikem/checkpoints/dummy3") \
  .start()

# COMMAND ----------

# MAGIC %scala 
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC def logEvent(event:String) = {
# MAGIC   val df:DataFrame = Seq((System.currentTimeMillis, "test")).toDF("time", event)
# MAGIC   df.write.format("delta").mode("append").saveAsTable("mikem.stream_log")
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.StreamingQueryListener
# MAGIC import org.apache.spark.sql.streaming.StreamingQueryListener._
# MAGIC 
# MAGIC spark.streams.addListener(new StreamingQueryListener() {
# MAGIC   override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
# MAGIC     logEvent("Query started")
# MAGIC   }
# MAGIC   override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
# MAGIC     logEvent("Query terminated")
# MAGIC   }
# MAGIC   override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
# MAGIC     logEvent("Query progress")
# MAGIC   }
# MAGIC })

# COMMAND ----------

# MAGIC %sql select * from mikem.stream_log

# COMMAND ----------

for s in spark.streams.active:
  print(s.name)
  s.stop()
