// Databricks notebook source
// MAGIC %sql
// MAGIC create database if not exists ds_xml_demo;
// MAGIC use ds_xml_demo;

// COMMAND ----------

val basePath = "dbfs:/FileStore/tables/test/xml/data" 
val ageData_path = s"$basePath/age"
val landing_folder = s"$ageData_path/landing";
val rawTableLocation = s"$ageData_path/raw";
val rawCheckPointLocation = s"$ageData_path/raw/checkpoint";

// COMMAND ----------

// DBTITLE 1,Dummy data
val xml = 
"""
<people>
  <person>
    <born>1990-02-24</born>
    <age>31</age>
  </person>
  <person>
    <born>1985-01-01</born>
    <age>36</age>
  </person>
</people>
"""

dbutils.fs.put("dbfs:/FileStore/tables/test/xml/data/age/landing/ages1.xml", xml)

// COMMAND ----------

// MAGIC %fs head dbfs:/FileStore/tables/test/xml/data/age/landing/ages1.xml

// COMMAND ----------

// MAGIC %md
// MAGIC #  Realtime XML Files ingestion with Autoloader

// COMMAND ----------

// MAGIC %md 
// MAGIC - `readStream` is used with binary and autoLoader listing mode options enabled.
// MAGIC - `toStrUDF` is used to convert binary data to string format (text).
// MAGIC - `from_xml` is used to convert the string to a complex struct type, with the user-defined schema.

// COMMAND ----------

import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import spark.implicits._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{input_file_name}

val toStrUDF = udf((bytes: Array[Byte]) => new String(bytes, "UTF-8")) // UDF to convert the binary to String

val text = spark.read.format("binaryFile")
                    .load(landing_folder)
                    .select(toStrUDF($"content")
                    .alias("text"))

display(text)

// COMMAND ----------

val schema = schema_of_xml(text.select("text").as[String])

// COMMAND ----------

val df = spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "binaryFile")
  .load(landing_folder)
  .select(toStrUDF($"content").alias("text"))
  .select(from_xml($"text", schema).alias("parsed"))
  .withColumn("path",input_file_name)

// COMMAND ----------

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val bronzeDF = df
   .withColumn("load_source", input_file_name) 
   .withColumn("load_timestamp", current_timestamp())
   .withColumn("load_date", to_date('load_timestamp))
   .writeStream
   .outputMode("append")
   .option("path", rawTableLocation)
   .option("checkpointLocation", rawCheckPointLocation)
   .toTable("ds_xml_demo.xml_raw")

// COMMAND ----------

// MAGIC %sql select * from ds_xml_demo.xml_raw

// COMMAND ----------

// MAGIC %sql
// MAGIC select A.born, A.age from ds_xml_demo.xml_raw as B
// MAGIC lateral view explode(B.parsed.person) person  as A

// COMMAND ----------

Thread.sleep(30*1000)
spark.streams.active.foreach(_.stop())

// COMMAND ----------

sql("drop table if exists ds_xml_demo.xml_raw")
dbutils.fs.rm(landing_folder, true)
dbutils.fs.rm(rawTableLocation, true)
dbutils.fs.rm(rawCheckPointLocation, true)

// COMMAND ----------


