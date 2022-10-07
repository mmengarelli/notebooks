# Databricks notebook source
# MAGIC %md # Job Params
# MAGIC You can pass parameters for your task. Each task type has different requirements for formatting and passing the parameters.
# MAGIC 
# MAGIC **Notebook**: Click Add and specify the key and value of each parameter to pass to the task. Parameters `set the value of the notebook widget` specified by the key of the parameter. Use task parameter variables to pass a limited set of dynamic values as part of a parameter value.
# MAGIC 
# MAGIC **JAR**: Use a JSON-formatted array of strings to specify parameters. These strings are passed as arguments to the main method of the main class. See Configure JAR job parameters.
# MAGIC 
# MAGIC **Spark Submit task**: Parameters are specified as a JSON-formatted array of strings. Conforming to the Apache Spark spark-submit convention, parameters after the JAR path are passed to the main method of the main class.
# MAGIC 
# MAGIC **Python**: Use a JSON-formatted array of strings to specify parameters. These strings are passed as arguments which can be parsed using the argparse module in Python.
# MAGIC 
# MAGIC **Python Wheel**: In the Parameters drop-down, select Positional arguments to enter parameters as a JSON-formatted array of strings, or select Keyword arguments > Add to enter the key and value of each parameter. Both positional and keyword arguments are passed to the Python wheel task as command-line arguments.

# COMMAND ----------

# MAGIC %fs ls /mnt/mikem/checkpoints/

# COMMAND ----------

schema = spark.read.json("/mnt/training/retail-org/solutions/sales_stream/12-05.json").schema

df = spark.readStream.schema(schema) \
  .format("json") \
  .option("path", "/mnt/training/retail-org/solutions/sales_stream") \
  .option("maxFilesPerTrigger", 1) \
  .load()

# COMMAND ----------

checkpoint_path = dbutils.widgets.get("checkpoint_path")
if checkpoint_path == "":
  raise Exception("checkpoint_path must be specified")
  
query_name = dbutils.widgets.get("query_name")
if checkpoint_path == "":
  raise Exception("query_name must be specified")  

# COMMAND ----------

df.writeStream \
  .format("console") \
  .trigger(processingTime='300 seconds') \
  .queryName(query_name) \
  .option("checkpointLocation", checkpoint_path) \
  .start()

# COMMAND ----------

for s in spark.streams.active:
  print(s.name)
#   #s.stop()
