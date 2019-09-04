// Databricks notebook source
// MAGIC %md # Real-time analysis of Wikipedia data
// MAGIC <pre>We will use <b>Spark Structured Streaming</b> to read Wikipedia edits (deltas) from a Kafka topic. 
// MAGIC The data will be transformed, in-flight, and then written to a <b>Databricks Delta</b> table for down-stream consumption.
// MAGIC </pre>
// MAGIC <br>
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/streaming-wikipedia.png"/>

// COMMAND ----------

// DBTITLE 1,Setup
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._ 

val schema = StructType(List(
  StructField("channel", StringType, true),
  StructField("comment", StringType, true),
  StructField("delta", IntegerType, true),
  StructField("flag", StringType, true),
  StructField("geocoding", StructType(List(
    StructField("city", StringType, true),
    StructField("country", StringType, true),
    StructField("countryCode2", StringType, true),
    StructField("countryCode3", StringType, true),
    StructField("stateProvince", StringType, true),
    StructField("latitude", DoubleType, true),
    StructField("longitude", DoubleType, true)
  )), true),
  StructField("isAnonymous", BooleanType, true),
  StructField("isNewPage", BooleanType, true),
  StructField("isRobot", BooleanType, true),
  StructField("isUnpatrolled", BooleanType, true),
  StructField("namespace", StringType, true),
  StructField("page", StringType, true),
  StructField("pageURL", StringType, true),
  StructField("timestamp", StringType, true),
  StructField("url", StringType, true),
  StructField("user", StringType, true),
  StructField("userURL", StringType, true),
  StructField("wikipediaURL", StringType, true),
  StructField("wikipedia", StringType, true)
))

dbutils.fs.rm("/mikem/delta/wikipedia_edits", true)
spark.sql("drop table if exists mikem.wikipedia_edits")

// COMMAND ----------

// DBTITLE 1,Read from stream
val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")
  .option("subscribe", "en") //Topic: EN - wikipedia edits
  .load()
  .select($"timestamp", $"value".cast("STRING").as("value"))

df.printSchema

// COMMAND ----------

// DBTITLE 1,Explode JSON
val json = df.select($"timestamp", from_json($"value", schema).as("json"))
json.printSchema

// COMMAND ----------

// MAGIC %md ### Continuous query

// COMMAND ----------

// DBTITLE 1,Select interesting fields
val edits = json.select(
  $"json.user".as("user"),
  unix_timestamp($"json.timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").as("timestamp"),
  $"json.page".as("page"),
  $"json.pageURL".as("pageURL"),
  $"json.namespace".as("namespace"), 
  $"json.geocoding".as("geocoding"))

// COMMAND ----------

display(edits)

// COMMAND ----------

// MAGIC %md ### Visualize edits

// COMMAND ----------

val filtered = edits
  .filter($"namespace" === "article")
  .filter(!isnull($"geocoding.countryCode3"))

// COMMAND ----------

// DBTITLE 1,Distribution of edits by country
val windowedCounts = filtered.groupBy(
  //10s window refreshing every 5s
  window($"timestamp", "10 seconds", "5 seconds"), $"geocoding.countryCode3")
  .count()
  .select($"countryCode3", $"count")

display(windowedCounts)

// COMMAND ----------

// MAGIC %md ### Save to Delta

// COMMAND ----------

val deltaPath = "/mikem/delta/wikipedia_edits"

// Use delta sink
edits.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/delta/events/checkpoints/wikipedia_edits") 
  .start(deltaPath)

// COMMAND ----------

spark.sql(s"""
  CREATE TABLE mikem.wikipedia_edits 
  USING DELTA 
  LOCATION '$deltaPath' 
""")

// COMMAND ----------

// DBTITLE 1,Distribution of edits by country - Delta
// MAGIC %sql 
// MAGIC select count(*), geocoding.countryCode3 from mikem.wikipedia_edits
// MAGIC where namespace = 'article' and geocoding.countryCode3 is not null
// MAGIC group by geocoding.countryCode3