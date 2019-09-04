// Databricks notebook source
// MAGIC %md-sandbox <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/delta-lake-logo.png" style="float:left; margin-right: 50px; height: 60px;"/> 
// MAGIC # Analyzing Flight Delays with Delta Lake
// MAGIC 
// MAGIC <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/air2.jpg" style="float:right; height: 250px; border: 1px solid #ddd; border-radius: 5px 5px 5px 5px; padding: 5px;"/>
// MAGIC 
// MAGIC **Delta Lake** enables reliability and data quality for your data lake. 
// MAGIC 
// MAGIC **Databricks Delta** provides many benefits over traditional data lakes including:
// MAGIC * ACID transactions
// MAGIC * Data reliability
// MAGIC * CRUD 
// MAGIC * Lineage 
// MAGIC * Time travel
// MAGIC 
// MAGIC <small>[delta.io](http://delta.io)</small>

// COMMAND ----------

// MAGIC %run ./delta_lake_webinar_setup

// COMMAND ----------

// MAGIC %md ## Explore

// COMMAND ----------

val flights = spark.read
  .option("header", "true") 
  .option("inferSchema", "true") 
  .csv("/mnt/mikem/asa/airlines")

flights.printSchema

// COMMAND ----------

flights.count

// COMMAND ----------

// MAGIC %md ## Data Engineering

// COMMAND ----------

val delays = flights
.drop("TaxiIn")
.drop("TaxiOut")
.drop("Cancelled")
.drop("CancellationCode")
.drop("Diverted")
.drop("ActualElapsedTime")
.drop("TailNum")
.filter("ArrDelay > 0 or DepDelay > 0")

delays.printSchema

// COMMAND ----------

delays.count

// COMMAND ----------

// MAGIC %md #### Create `Delta` table

// COMMAND ----------

// DBTITLE 0,Write Parquet and Delta tables
delays.write.format("delta").partitionBy("DEST").save(deltaPath)

// COMMAND ----------

// MAGIC %sql 
// MAGIC create table delayed_flights 
// MAGIC using DELTA 
// MAGIC location '/mnt/delta/delayed_flights_delta'

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Notice how simple the syntax was?

// COMMAND ----------

// MAGIC %md #### Alt: Convert to `Delta`

// COMMAND ----------

convert to delta parquet.`/mnt/mikem/asa/delays2008` partitioned by (origin string)

// COMMAND ----------

// MAGIC %md #### Now let's query our `Delta` table

// COMMAND ----------

// DBTITLE 1,Top 5 destinations with delays
// MAGIC %sql select count(*) as ct, dest from delayed_flights 
// MAGIC group by dest
// MAGIC order by ct desc 
// MAGIC limit 5

// COMMAND ----------

// MAGIC %md ## Updates

// COMMAND ----------

val updates0901 = spark.read.parquet("/mnt/mikem/asa/updates/200901")
display(updates0901)

// COMMAND ----------

// MAGIC %md #### Schema Validation

// COMMAND ----------

updates0901.write.mode("append").format("delta").saveAsTable("delayed_flights")

// COMMAND ----------

// DBTITLE 1,The fix
val updatesFixed = updates0901.withColumn("Date", to_timestamp('Date, "yyyy/MM/dd"))
updatesFixed.printSchema

// COMMAND ----------

updatesFixed.write.mode("append").format("delta").saveAsTable("delayed_flights")

// COMMAND ----------

// MAGIC %md #### Schema Evolution
// MAGIC 
// MAGIC Delta Lake supports schema evolution with the `mergeSchema` and `overwriteSchema` options

// COMMAND ----------

val updates02 = spark.read.parquet("/mnt/mikem/asa/updates/200902")
updates02.printSchema

// COMMAND ----------

updates02.write.mode("append").format("delta").saveAsTable("delayed_flights")

// COMMAND ----------

updates02.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("delayed_flights")

// COMMAND ----------

display(spark.table("delayed_flights").where("passdelay > 0"))

// COMMAND ----------

// MAGIC %md #### UPSERT

// COMMAND ----------

// MAGIC %md-sandbox <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/font-load.png"/> Backfill: Modifications to `WeatherDelay` for `Dest = ORD and Date = 2008-12-25` data and additions to `2009-03`

// COMMAND ----------

val updates03 = spark.read.parquet("/mnt/mikem/asa/updates/200903")
updates03.createOrReplaceTempView("updates03")

display(spark.table("updates03").select(year('Date), month('Date)).distinct())

// COMMAND ----------

// DBTITLE 0,UPSERT
// MAGIC %sql
// MAGIC MERGE INTO delayed_flights
// MAGIC USING updates03
// MAGIC  ON updates03.FlightNum = delayed_flights.FlightNum 
// MAGIC  and updates03.carrier = delayed_flights.carrier 
// MAGIC  and updates03.Date = delayed_flights.Date 
// MAGIC WHEN MATCHED THEN UPDATE SET * 
// MAGIC WHEN NOT MATCHED THEN INSERT * 

// COMMAND ----------

// MAGIC %sql select WeatherDelay from delayed_flights 
// MAGIC where Dest = 'ORD' and Date = '2008-12-25'
// MAGIC order by flightnum

// COMMAND ----------

// MAGIC %sql select count(*) from delayed_flights where dest = 'DFW' and month(date) = 3 and year(date) = 2009

// COMMAND ----------

// MAGIC %md ## Lineage

// COMMAND ----------

// MAGIC %sql desc history delayed_flights

// COMMAND ----------

// MAGIC %md ## Time Travel

// COMMAND ----------

// DBTITLE 0,Time Travel
// MAGIC %sql select WeatherDelay from delayed_flights
// MAGIC version as of 0
// MAGIC where Dest = 'ORD' and Date = '2008-12-25'
// MAGIC order by flightnum

// COMMAND ----------

// MAGIC %md-sandbox 
// MAGIC #### Additional Topics & Resources
// MAGIC <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/delta-lake-logo.png" style="float:left; margin-right: 55px; height: 55px;"/>
// MAGIC *   <a href="https://docs.delta.io/latest/index.html" target="_blank">Delta Lake Documentation</a>
// MAGIC *   <a href="https://delta.io" target="_blank">Delta Lake Website</a>