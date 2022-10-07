// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

// COMMAND ----------

sql("drop table if exists mikem.results")

// COMMAND ----------

val schema = table("mikem.assets").schema

// COMMAND ----------

val sdf = spark.readStream                       
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet("/mnt/mikem/tmp/assets")

// COMMAND ----------

def boolToInt(b: Boolean): Int = {
  if (b) 1 else 0
}

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

val sdf2 = sdf.withColumn("InfLag", lag("Inference", 1).over(Window.partitionBy('Asset_ID).orderBy('Timestamp)))
  .withColumn("Grouping",sum(lit(boolToInt('Inference != 'InFlag))).over(Window.partitionBy('Asset_ID).orderBy('Timestamp)))
  .withColumn("StreakStart", min('Timestamp).over(Window.partitionBy('Asset_ID, 'Grouping)))
  .withColumn("InferenceStreak", 'Timestamp.cast("long") - 'StreakStart.cast("long"))
  .withColumn("AnomalyRatePast6H", sum(lit(boolToInt('Inference==0)))
    .over(Window.partitionBy('Asset_ID).orderBy(unix_timestamp('Timestamp)).rangeBetween(-21599, 0)) / lit(24.0))
  .withColumn("AnomalyRatePast24H", sum(lit(boolToInt('Inference==0)))
    .over(Window.partitionBy('Asset_ID).orderBy(unix_timestamp('Timestamp)).rangeBetween(-86399, 0)) / lit(96.0))

// COMMAND ----------

// sdf2.writeStream.format("delta")
//   .outputMode("append")
//   .option("checkpointLocation", "/delta/events/_checkpoints/etl-from-json")
//   .table("mikem.results")

// COMMAND ----------

case class InputRow(Asset_ID:Int, Inference:Int, Timestamp:Long)

case class EventState(
  var Asset_ID:Int = 1, 
  var Inference:Int = 1, 
  var Timestamp:Long = 1223333L,
  var InfLag:Int = 1,
  var Grouping:Int = 1,
  var StreakStart:java.sql.Date = new java.sql.Date(System.currentTimeMillis()),
  var InferenceStreak:Long = 12345676L,
  var AnomalyRatePast6H:Int = 1,
  var AnomalyRatePast24H:Int = 1
)

// COMMAND ----------

import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.Row

def updateAcrossEvents(num:Int, rows:Iterator[Row], groupState:GroupState[EventState]):EventState={
  val es:EventState = EventState(1,1,1000000,1,1,new java.sql.Date(System.currentTimeMillis()),1000000,1,1)
  es
}

// COMMAND ----------

case class InputRow(Asset_ID:Int, Inference:Int, Timestamp:Long)

def updateEventState(state:EventState):EventState={
  val es:EventState = EventState(
    Asset_ID=state.Asset_ID, 
    Inference=state.Inference, 
    Timestamp=state.Timestamp,
    InfLag=2,
    Grouping=3,
    StreakStart=new java.sql.Date(System.currentTimeMillis()),
    InferenceStreak=1234356,
    AnomalyRatePast6H=1,
    AnomalyRatePast24H=2
  )
  es
}

// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}

def updateAcrossEvents(user:String,
  inputs: Iterator[InputRow],
  oldState: GroupState[EventState]):EventState = {
    var evtState:EventState = if (oldState.exists) oldState.get else EventState()

    for (input <- inputs) {
      evtState = updateEventState(evtState)
      oldState.update(evtState)
    }
  evtState
}

// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout

// for each org.apache.spark.sql.Row
val sdf3 = sdf.groupByKey(_.getInt(0)).mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)

// COMMAND ----------

sdf3.writeStream.queryName("events_state")
  .format("memory")
  .outputMode("update")
  .start()

// COMMAND ----------

// MAGIC %sql select * from events_state
