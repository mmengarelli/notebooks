// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/delta-lake-logo.png" style="float:left; margin-right: 50px; height: 60px;"/> 
// MAGIC # Analyzing Flight Delays with Delta Lake
// MAGIC 
// MAGIC <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/air2.jpg" style="float:right; height: 250px; border: 1px solid #ddd; border-radius: 5px 5px 5px 5px; padding: 5px;"/>
// MAGIC 
// MAGIC Building robust, high performance data pipelines can be difficult due to: _lack of indexing and statistics_, _data inconsistencies introduced by schema changes_ and _pipeline failures_, _and having to trade off between batch and stream processing_.
// MAGIC 
// MAGIC With **Databricks Delta**, data engineers can build reliable and fast data pipelines. **Databricks Delta** provides many benefits including:
// MAGIC * Faster query execution with indexing, statistics, and auto-caching support
// MAGIC * Data reliability with rich schema validation and transactional guarantees
// MAGIC * Simplified data pipeline with flexible `UPSERT` support and unified Structured Streaming + batch processing on a single data source.
// MAGIC 
// MAGIC <small>[Delta Blog](https://databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)</small>
// MAGIC 
// MAGIC <!--
// MAGIC # delta
// MAGIC # airline demo
// MAGIC # demo-ready
// MAGIC # mm-demo
// MAGIC -->

// COMMAND ----------

// DBTITLE 1,Setup
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.shuffle.partitions", "32")

spark.range(1, 50).foreachPartition{_ => dbutils.fs.rm("/tmp/delayed_flights_delta", true)}
spark.range(1, 50).foreachPartition{_ => dbutils.fs.rm("/tmp/delayed_flights_parquet", true)}

spark.sql("drop table if exists mikem.delayed_flights")

// COMMAND ----------

// MAGIC %md ## Explore

// COMMAND ----------

// DBTITLE 0,Read flights data
val flights = spark.read
  .option("header", "true") 
  .option("inferSchema", "true") 
  .csv("/mnt/mcm/flights08")

flights.printSchema

// COMMAND ----------

flights.count

// COMMAND ----------

// MAGIC %md ## Data Engineering

// COMMAND ----------

// DBTITLE 0,Data Engineering
val delayedFlights = flights
 .withColumnRenamed("UniqueCarrier", "Carrier")
 .withColumn("DateStr", concat('year, lit('-'), 'month, lit('-'), 'dayofmonth))
 .withColumn("Date", to_date('DateStr, "yyyy-M-d"))
 .where("ArrDelay > 0 or DepDelay > 0")
 .select('Date, 'Carrier,'FlightNum, 'Origin, 'Dest, 'DepTime, 'ArrTime, 'Distance, 'WeatherDelay)

display(delayedFlights)

// COMMAND ----------

delayedFlights.count

// COMMAND ----------

// MAGIC %md ### Create `Delta` table

// COMMAND ----------

// DBTITLE 0,Write Parquet and Delta tables
delayedFlights.repartition('Origin)
  .write.format("parquet").partitionBy("Origin").save("/tmp/delayed_flights_parquet")

delayedFlights.repartition('Origin)
  .write.format("delta").partitionBy("Origin").save("/tmp/delayed_flights_delta")

// COMMAND ----------

// MAGIC %sql 
// MAGIC create table mikem.delayed_flights 
// MAGIC using DELTA 
// MAGIC location '/tmp/delayed_flights_delta'

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how simple the syntax was?

// COMMAND ----------

// MAGIC %md ### Alt: Convert to `Delta`

// COMMAND ----------

// MAGIC %sql -- convert to delta parquet.`/tmp/delayed_flights_parquet` partitioned by (origin string)

// COMMAND ----------

// MAGIC %md ### Now let's query our `Delta` table

// COMMAND ----------

// DBTITLE 1,Top 5 airports with delays
// MAGIC %sql select count(*) as ct, origin from mikem.delayed_flights 
// MAGIC group by origin
// MAGIC order by ct desc 
// MAGIC limit 5

// COMMAND ----------

// MAGIC %md ## Update

// COMMAND ----------

// MAGIC %sql select * from mikem.delayed_flights where flightnum = 72 and carrier = 'UA'

// COMMAND ----------

// MAGIC %sql 
// MAGIC update mikem.delayed_flights 
// MAGIC set distance = 375 
// MAGIC where flightnum = 72 and carrier = 'UA'

// COMMAND ----------

// MAGIC %md-sandbox <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/font-load.png"/> Backfill: Provide some modifications and additions

// COMMAND ----------

val flights = delayedFlights.filter("Dest = 'ORD'").filter("Date = '2008-12-25'")
val updates = flights.withColumn("WeatherDelay", lit(99))
val additions = flights.withColumn("DateStr", lit("2009-01-01")).withColumn("Date", to_date($"DateStr")).drop("DateStr")

val backfills = updates.union(additions)
backfills.createOrReplaceTempView("backfills")

// COMMAND ----------

// MAGIC %md ## Upsert

// COMMAND ----------

// DBTITLE 0,UPSERT
// MAGIC %sql
// MAGIC MERGE INTO mikem.delayed_flights
// MAGIC USING backfills
// MAGIC ON backfills.FlightNum = delayed_flights.FlightNum 
// MAGIC and backfills.carrier = delayed_flights.carrier 
// MAGIC and backfills.Date = delayed_flights.Date 
// MAGIC WHEN MATCHED THEN 
// MAGIC  UPDATE SET delayed_flights.WeatherDelay = backfills.WeatherDelay
// MAGIC WHEN NOT MATCHED
// MAGIC THEN INSERT * 

// COMMAND ----------

// MAGIC %sql select count(*) from mikem.delayed_flights where date = '2009-01-01'

// COMMAND ----------

// MAGIC %sql select * from mikem.delayed_flights 
// MAGIC where Dest = 'ORD' and Date = '2008-12-25'
// MAGIC order by flightnum

// COMMAND ----------

// MAGIC %md ## Lineage

// COMMAND ----------

// MAGIC %sql desc history mikem.delayed_flights

// COMMAND ----------

// MAGIC %md ## Time Travel

// COMMAND ----------

// DBTITLE 0,Time Travel
// MAGIC %sql select * from mikem.delayed_flights
// MAGIC version as of 0
// MAGIC where Dest = 'ORD' and Date = '2008-12-25'
// MAGIC order by flightnum

// COMMAND ----------

// MAGIC %md ## ZORDER: Improving Read Performance
// MAGIC 
// MAGIC <img src="https://databricks.com/wp-content/uploads/2018/07/Screen-Shot-2018-07-30-at-2.03.55-PM.png" style="height: 150px; position: absolute; left: 900px; top: 25px;"/>

// COMMAND ----------

// MAGIC %sql select * from mikem.delayed_flights where carrier = 'AA' 

// COMMAND ----------

// MAGIC %sql optimize mikem.delayed_flights zorder by carrier

// COMMAND ----------

// MAGIC %md ## Visualization
// MAGIC * Browse Tableau Dashboard
// MAGIC * Browse [Superset Dashboard](http://0.0.0.0:8088/superset/dashboard/delayedflights/)