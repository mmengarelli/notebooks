# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # Troubleshooting DLT Python Syntax
# MAGIC 
# MAGIC Now that we've gone through the process of configuring and running a pipeline with 2 notebooks, we'll simulate developing and adding a 3rd notebook.
# MAGIC 
# MAGIC **DON'T PANIC!**
# MAGIC 
# MAGIC The code provided below contains some intentional, small syntax errors. By troubleshooting these errors, you'll learn how to iteratively develop DLT code and identify errors in your syntax.
# MAGIC 
# MAGIC This lesson is not meant to provide a robust solution for code development and testing; rather, it is intended to help users getting started with DLT and struggling with an unfamiliar syntax.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, students should feel comfortable:
# MAGIC * Identifying and troubleshooting DLT syntax 
# MAGIC * Iteratively developing DLT pipelines with notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add this Notebook to a DLT Pipeline
# MAGIC 
# MAGIC At this point in the course, you should have a DLT Pipeline configured with 2 notebook library.
# MAGIC 
# MAGIC You should have processed several batches of records through this pipeline, and should understand how to trigger a new run of the pipeline and add an additional library.
# MAGIC 
# MAGIC To begin this lesson, go through the process of adding this notebook to your pipeline using the DLT UI, and then trigger an update.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting Errors
# MAGIC 
# MAGIC Each of the 3 functions below contains a syntax error, but each of these errors will be detected and reported slightly differently by DLT.
# MAGIC 
# MAGIC Some syntax errors will be detected during the **Initializing** stage, as DLT is not able to properly parse the commands.
# MAGIC 
# MAGIC Other syntax errors will be deteced during the **Setting up tables** stage.
# MAGIC 
# MAGIC Note that because of the way DLT resolves the order of tables in the pipeline at different steps, you may sometimes see errors thrown for later stages first.
# MAGIC 
# MAGIC An approach that can work well is to fix one table at a time, starting at your earliest dataset and working toward your final. Commented code will be ignored automatically, so you can safely remove code from a development run without removing it entirely.
# MAGIC 
# MAGIC Even if you can immediately spot the errors in the code below, try to use the error messages from the UI to guide your identification of these errors. Solution code follows in the cell below.

# COMMAND ----------

import pyspark.sql.functions as F

source = spark.conf.get("source")


def status_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{source}/status")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

    
@dlt.table(
    table_name = "status_silver"
    )
@dlt.expect_or_drop("valid_timestamp", "status_timestamp > 1640995200")
def status_silver():
    return (
        dlt.read_stream("status_bronze")
            .drop("source_file", "_rescued_data")
    )

    
@dlt.table
def email_updates():
    return (
        spark.readStream("status_silver").alias("a")
            .join(
                dlt.read_stream("subscribed_order_emails").alias("b"), 
                on="order_id"
            ).select(
                "a.*", 
                "b.email"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions
# MAGIC 
# MAGIC The correct syntax for each of our above functions is provided below.
# MAGIC 
# MAGIC You can either fix the code above, or comment out/delete that cell and uncomment the code below.
# MAGIC 
# MAGIC **NOTE**: You won't be able to see any of the other errors until you add the `import dlt` statement to the cell above.
# MAGIC 
# MAGIC The issues in each query:
# MAGIC 1. The `@dlt.table` decorator is missing before the function definition
# MAGIC 1. The correct keyword argument to provide a custom table name is `name` not `table_name`
# MAGIC 1. To perform a streaming read on a table in the pipeline, use `dlt.read_stream` not `spark.readStream`

# COMMAND ----------

# import dlt
# import pyspark.sql.functions as F

# source = spark.conf.get("source")


# @dlt.table
# def status_bronze():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .load(f"{source}/status")
#             .select(
#                 F.current_timestamp().alias("processing_time"), 
#                 F.input_file_name().alias("source_file"), 
#                 "*"
#             )
#     )

    
# @dlt.table(
#         name = "status_silver"
#     )
# @dlt.expect_or_drop("valid_timestamp", "status_timestamp > 1640995200")
# def status_silver():
#     return (
#         dlt.read_stream("status_bronze")
#             .drop("source_file", "_rescued_data")
#     )

    
# @dlt.table
# def email_updates():
#     return (
#         spark.read_stream("status_silver").alias("a")
#             .join(
#                 dlt.read_stream("subscribed_order_emails").alias("b"), 
#                 on="order_id"
#             ).select(
#                 "a.*", 
#                 "b.email"
#             )
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC By reviewing this notebook, you should now feel comfortable:
# MAGIC * Identifying and troubleshooting DLT syntax 
# MAGIC * Iteratively developing DLT pipelines with notebooks
