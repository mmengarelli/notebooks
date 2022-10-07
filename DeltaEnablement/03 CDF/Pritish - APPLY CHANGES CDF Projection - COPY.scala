// Databricks notebook source
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._

// read as a stream
val changeDataFrame = spark.readStream.format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 0)
  .load("/pipelines/f414b918-7a2a-4a72-8820-6bca7fa01dd5/tables/target") // path to raw live table. - can be found in pipeline.
  .select(
    col("userId"), col("name"), col("city"), col("timestamp"),
    when(col("_change_type") === lit("update_postimage"), lit(2) * col("_commit_version") + lit(1)).otherwise(lit(2) * col("_commit_version")).as("_commit_version"),
    when(col("_change_type") === lit("update_preimage"), lit("delete")).when(col("_change_type") === lit("update_postimage"), lit("insert")).otherwise(col("_change_type")).as("_change_type"),
    col("_commit_timestamp")
  ).where(col("__DeleteVersion").isNull)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE yijia.recompute(userId INT, name STRING, city STRING, timestamp DATE) using delta

// COMMAND ----------

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql._

// foreach batch with CDF streaming query.
val deltaTable = DeltaTable.forName(spark, "yijia.recompute")

val q = changeDataFrame.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  val cols = Seq("userId", "timestamp", "name", "city").map { n => n -> batchDF.col(n) }
  // suppose deviceId is the key. In the streaming batch, the one with the latest commit version should be taken to update the table.
  val sortedWindow = Window.partitionBy("userId").orderBy(col("_commit_version").desc)
  val sourceDf = batchDF.withColumn("__rowNumber", row_number().over(sortedWindow)).where("__rowNumber = 1").drop("__rowNumber")
  // suppose deviceId is the key and also timestamp won't be updated.
  deltaTable.as("tgt").merge(sourceDf.as("src"), condition = "src.userId = tgt.userId")
      .whenMatched("src._change_type = 'insert'").update(cols.toMap)
      .whenMatched("src._change_type = 'delete'").delete()
      .whenNotMatched("src._change_type = 'insert'").insert(cols.toMap) // if there is no match but the event is insert. insert the record.
      .execute()
  
  // As we converted update into delete + insert, if there is an update event on a device, the change on that day will be -1 + 1 = 0.
  // if there is a device inserted, the change on that day will be + 1.
  // if there is a device deleted, the change on that day will be - 1.
  // It is possible that a update event are ingested in two batches, the result of the recomputation will be eventually the same.
  
  // Note: here is only the # of devices changed in this batch.
  val cntDF = batchDF.withColumn("cnt", when(col("_change_type") === lit("insert"), lit(1)).otherwise(lit(-1)))
  val diffOnDate = cntDF.groupBy("timestamp").sum("cnt")
  // 7-days window for counts.
  val sevenDaysChanges = cntDF.groupBy(window(col("timestamp"), "7 days", "1 day")).sum("cnt")
}.start
q.processAllAvailable()

// COMMAND ----------

// batch read.
spark.read.format("delta")
  .option("readChangeFeed", "true")
  .option("startingVersion", 0)
  .load("/pipelines/f414b918-7a2a-4a72-8820-6bca7fa01dd5/tables/target") // path to raw live table. - can be found in pipeline.
  .select(
    col("userId"), col("name"), col("city"),
    when(col("_change_type") === lit("update_postimage"), lit(2) * col("_commit_version") + lit(1)).otherwise(lit(2) * col("_commit_version")).as("_commit_version"),
    when(col("_change_type") === lit("update_preimage"), lit("delete")).when(col("_change_type") === lit("update_postimage"), lit("insert")).otherwise(col("_change_type")).as("_change_type"),
    col("_commit_timestamp")
  ).where(col("__DeleteVersion").isNull)
