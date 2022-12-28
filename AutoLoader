# Databricks notebook source
# MAGIC %md # Generate Metadata
# MAGIC Creates `{input path, output table name and write mode}` definitions for each table in the database

# COMMAND ----------

# MAGIC %sql
# MAGIC use mikem;
# MAGIC drop table if exists autoloader_config;
# MAGIC 
# MAGIC create table autoloader_config (
# MAGIC     source_path string, 
# MAGIC     destination_table string,
# MAGIC     mode string
# MAGIC );
# MAGIC 
# MAGIC insert into autoloader_config values 
# MAGIC     ("/mnt/mikem/data/autoloader1", "autoloader1", "append"),
# MAGIC     ("/mnt/mikem/data/autoloader2", "autoloader2", "append"),
# MAGIC     ("/mnt/mikem/data/autoloader3", "autoloader3", "append"),
# MAGIC     ("/mnt/mikem/data/autoloader4", "autoloader4", "append"),
# MAGIC     ("/mnt/mikem/data/autoloader5", "autoloader5", "overwrite")

# COMMAND ----------

config_df = spark.table("mikem.autoloader_config")
row_list = config_df.collect()
display(row_list)

# COMMAND ----------

# MAGIC %md # Function Definitions

# COMMAND ----------

def run_notebook(notebook, timeout, params):
  dbutils.notebook.run(notebook, timeout, params)

# COMMAND ----------

# MAGIC %md # Generate Dummy Data for Each Autoloader Path

# COMMAND ----------

for row in row_list:
  run_notebook("DataGenParameterized", 90, {"path": row.source_path})

# COMMAND ----------

# MAGIC %md # Run Autloader Notebook Serially

# COMMAND ----------

for row in row_list:
  run_notebook("AutoLoaderParameterized", 900, 
    {"source": row.source_path, "destination": row.destination_table, "mode": row.mode})

# COMMAND ----------

# MAGIC %md # Run Autoloader Notebooks in Parallel
# MAGIC 
# MAGIC Notes:
# MAGIC * Size cluster accordingly
# MAGIC * Perhaps process chunks of 20/30 datasets at a time

# COMMAND ----------

# MAGIC %md ## 1. Python Multiprocessing

# COMMAND ----------

import concurrent.futures
import multiprocessing

rows = [
  ("/mnt/mikem/data/autoloader1", "autoloader1", "append"),
  ("/mnt/mikem/data/autoloader2", "autoloader2", "append"),
  ("/mnt/mikem/data/autoloader3", "autoloader3", "append"),
  ("/mnt/mikem/data/autoloader4", "autoloader4", "append"),
  ("/mnt/mikem/data/autoloader5", "autoloader5", "overwrite")
]

def run_parallel(df):
  run_notebook("AutoLoaderParameterized", 900, 
    {"source": df[0], "destination": df[1], "mode": df[2]})

# COMMAND ----------

# DBTITLE 1,Run notebooks concurrently
num_workers = multiprocessing.cpu_count() - 1
with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
  results = list(executor.map(run_parallel, rows))

print(results)

# COMMAND ----------

# MAGIC %md ## 2. RDD API: Flat Map
# MAGIC 
# MAGIC ⚠️ Not possible. Cannot use `dbutils` on executor

# COMMAND ----------

config_df.rdd.flatMap(
  lambda r: run_notebook(r.source_path, r.destination_table, r.mode)
).count()