# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # Land New Data
# MAGIC 
# MAGIC This notebook is provided for the sole purpose of manually triggering new batches of data to be processed by an already configured Delta Live Tables pipeline.
# MAGIC 
# MAGIC Note that the logic provided is identical to that provided in the first interactive notebook, but does not reset the source or target directories for the pipeline.

# COMMAND ----------

# MAGIC %run ./Includes/setup 

# COMMAND ----------

# MAGIC %md
# MAGIC Each time this cell is run, a new batch of data files will be loaded into the source directory used in these lessons.

# COMMAND ----------

File.new_data()
