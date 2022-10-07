// Databricks notebook source
import java.sql.Timestamp

case class Row(asset_id:Int, inference:Int, timestamp:java.sql.Timestamp)

case class EventState(
  var seq:Int, // seq num for debugging
  var asset_id:Int, 
  var inference:Int, 
  var timestamp:Timestamp,
  var infLag:Int,
  var grouping:Int,
  var streakStart:Timestamp,
  var inferenceStreak:Long,
  var anomalyRatePast6H:Int,
  var anomalyRatePast24H:Int
)

def newTimestamp() : Timestamp = {
  new java.sql.Timestamp(System.currentTimeMillis())
}

def initEventState() : EventState = {
  new EventState(0,
                0,
                0, 
                newTimestamp(),
                0,
                0,
                newTimestamp(),
                0L,
                0,
                0)
}

// COMMAND ----------

// DBTITLE 1,Original code
// import org.apache.spark.sql.expressions.Window

// val sdf2 = sdf.withColumn("InfLag", lag("Inference", 1).over(Window.partitionBy('Asset_ID).orderBy('Timestamp)))
//   .withColumn("Grouping",sum(lit(boolToInt('Inference != 'InFlag))).over(Window.partitionBy('Asset_ID).orderBy('Timestamp)))
//   .withColumn("StreakStart", min('Timestamp).over(Window.partitionBy('Asset_ID, 'Grouping)))
//   .withColumn("InferenceStreak", 'Timestamp.cast("long") - 'StreakStart.cast("long"))
//   .withColumn("AnomalyRatePast6H", sum(lit(boolToInt('Inference==0)))
//     .over(Window.partitionBy('Asset_ID).orderBy(unix_timestamp('Timestamp)).rangeBetween(-21599, 0)) / lit(24.0))
//   .withColumn("AnomalyRatePast24H", sum(lit(boolToInt('Inference==0)))
//     .over(Window.partitionBy('Asset_ID).orderBy(unix_timestamp('Timestamp)).rangeBetween(-86399, 0)) / lit(96.0))

// COMMAND ----------

// DBTITLE 1,Aggregation logic 
import org.apache.spark.sql.expressions.Window

def updateAssetStateWithEvent(state:EventState, input:Row):EventState = {
  state.seq += 1
  state.asset_id = input.asset_id
  state.inference = input.inference
  state.timestamp = input.timestamp
  //state.infLag = ?
  //state.grouping = ? 
  
  if(input.inference == 1) {
      if(state.inference == 0) {
        state.streakStart = newTimestamp()
      }
      state.inferenceStreak = (newTimestamp().getTime() - state.streakStart.getTime())
      //Increment rate?
      state.anomalyRatePast6H += 1 // filter on 6hrs
      state.anomalyRatePast24H += 1 // filter on 24 hrs
    }
  state
}

// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}

def updateAcrossEvents(asset_id:Int, inputs: Iterator[Row], oldState: GroupState[EventState]):EventState = {
  if(oldState.hasTimedOut){
    oldState.remove()
    //return??
  }
  
  var isNew = false
  var newState:EventState = if(oldState.exists) {
    oldState.get 
  } else {
    initEventState()
  }
  
  if(newState.seq == 0) oldState.setTimeoutDuration("24 hours") 

  for (input <- inputs) {
    newState = updateAssetStateWithEvent(newState, input)
    oldState.update(newState)
  }
  
  newState
}

// COMMAND ----------

// MAGIC %md ##### Read stream

// COMMAND ----------

// just to get the scheam for the stream
val schema = table("mikem.assets").schema

// COMMAND ----------

// mock stream
val streaming = spark.readStream                       
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .parquet("/mnt/mikem/tmp/assets")

// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout

val withEventTime = streaming
  .selectExpr("asset_id", "inference", "timestamp")
  .as[Row]
  .groupByKey(_.asset_id)
  .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("update")
  .start()

// COMMAND ----------

// MAGIC %sql select * from events_per_window where asset_id = 1 order by seq

// COMMAND ----------


