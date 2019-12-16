// Databricks notebook source
// MAGIC %md # Flight Delay CI/CD + DB Connect Demo

// COMMAND ----------

// DBTITLE 1,Setup
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.shuffle.partitions", "32")

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

println("Flight Count: " + flights.count)

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

println("Delayed Flights Count: " + delayedFlights.count())