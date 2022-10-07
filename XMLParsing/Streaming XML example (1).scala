// Databricks notebook source
val xml2="""<people>
  <person>
    <age born="1990-02-24">25</age>
  </person>
  <person>
    <age born="1985-01-01">30</age>
  </person>
  <person>
    <age born="1980-01-01">30</age>
  </person>
</people>"""

dbutils.fs.put("/tmp/xml/ages4.xml",xml2,true)

// COMMAND ----------

val xml3="""<people>
  <person>
    <age born="1990-02-24">25</age>
    <name>Hyukjin</name>
  </person>
  <person>
    <age born="1985-01-01"></age>
  </person>
  <person>
    <age born="1980-01-01">30</age>
  </person>
</people>"""

dbutils.fs.put("/tmp/xml/ages5.xml",xml3,true)

// COMMAND ----------

import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import spark.implicits._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val df_schema = spark.read.format("xml").option("rowTag", "people").load("/tmp/xml/")
val payloadSchema = df_schema.schema

// COMMAND ----------

payloadSchema.simpleString

// COMMAND ----------

val df = spark.readStream.format("cloudFiles")
  .option("cloudFiles.useNotifications", "false")
  .option("cloudFiles.format", "text")
  .option("wholeText", "true")
  .load("/tmp/xml/")
  .select(from_xml('value, payloadSchema).alias("parsed"))
  .select(explode('parsed.getItem("person")))
  .select(expr("col.*"))

// COMMAND ----------

display(df)

// COMMAND ----------

spark.streams.active.foreach(_.stop())
