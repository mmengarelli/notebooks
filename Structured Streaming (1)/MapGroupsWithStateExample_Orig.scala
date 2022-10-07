// Databricks notebook source
case class InputRow(user:String, timestamp:java.sql.Timestamp, activity:String)

case class UserState(
  user:String,
  var activity:String,
  var start:java.sql.Timestamp,
  var end:java.sql.Timestamp)

// COMMAND ----------

def updateUserStateWithEvent(state:UserState, input:InputRow):UserState = {
// no timestamp, just ignore it  
if (Option(input.timestamp).isEmpty) {
    return state
  }
//does the activity match for the input row
if (state.activity == input.activity) {
    if (input.timestamp.after(state.end)) {
      state.end = input.timestamp
    }
    if (input.timestamp.before(state.start)) {
      state.start = input.timestamp
    }
  } else { 
   //some other activity
    if (input.timestamp.after(state.end)) {
      state.start = input.timestamp
      state.end = input.timestamp
      state.activity = input.activity
    }
  }
  //return the updated state
  state
}

// COMMAND ----------

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, GroupState}

def updateAcrossEvents(user:String,
    inputs: Iterator[InputRow],
     oldState: GroupState[UserState]):UserState = {
     var state:UserState = if (oldState.exists) oldState.get else UserState(user,
        "",
        new java.sql.Timestamp(6284160000000L),
        new java.sql.Timestamp(6284160L)
    )
  // we simply specify an old date that we can compare against and
  // immediately update based on the values in our data

  for (input <- inputs) {
    state = updateUserStateWithEvent(state, input)
    oldState.update(state)
  }
  state
}

// COMMAND ----------

spark.read.json(path).printSchema

// COMMAND ----------

val path = "/databricks-datasets/definitive-guide/data/activity-data"
val static = spark.read.json(path)

val streaming = spark
  .readStream
  .schema(static.schema)
  .option("maxFilesPerTrigger", 1)
  .json(path)


// COMMAND ----------

import org.apache.spark.sql.streaming.GroupStateTimeout

val withEventTime = streaming
  .selectExpr("User as user", "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
  .as[InputRow]
  // group the state by user key
  .groupByKey(_.user)
  .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateAcrossEvents)
  .writeStream
  .queryName("events_per_window")
  .format("memory")
  .outputMode("update")
  .start()

// COMMAND ----------

// MAGIC %sql SELECT * FROM events_per_window order by user, start
