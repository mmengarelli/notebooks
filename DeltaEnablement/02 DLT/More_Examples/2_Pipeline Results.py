# Databricks notebook source
# INCLUDE_HEADER_FALSE
# INCLUDE_FOOTER_FALSE

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the Results of a DLT Pipeline
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC While DLT abstracts away many of the complexities associated with running production ETL on Databricks, many folks may wonder what's actually happening under the hood.
# MAGIC 
# MAGIC In this notebook, we'll avoid getting too far into the weeds, but will explore how data and metadata are persisted by DLT.

# COMMAND ----------

# MAGIC %run ./Includes/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying Tables in the Target Database
# MAGIC 
# MAGIC As long as a target database is specified during DLT Pipeline configuration, tables should be available to users throughout your Databricks environment.
# MAGIC 
# MAGIC Run the cell below to see the tables registered to the database used in this demo.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${da.database};
# MAGIC 
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the two views we defined in our pipeline (both with `_v` suffixes) are absent from our tables list.
# MAGIC 
# MAGIC Query results from the `orders_bronze` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Recall that `orders_bronze` was defined as a streaming table in DLT, but our results here are static.
# MAGIC 
# MAGIC Because DLT uses Delta Lake to store all tables, each time a query is executed, we will always return the most recent version of the table. But queries outside of DLT will return snapshot results from DLT tables, regardless of how they were defined.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Results of `APPLY CHANGES INTO`
# MAGIC 
# MAGIC Recall that the `customers_silver` table was implemented with changes from a CDC feed applied as Type 1 SCD.
# MAGIC 
# MAGIC Let's query this table below.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Note the `customers_silver` table correctly represents the current active state of our Type 1 table with changes applied, but does not include the additional fields seen in the schema shown in the DLT UI: **__Timestamp**, **__DeleteVersion**, and **__UpsertVersion**.
# MAGIC 
# MAGIC This is because our `customers_silver` table is actually implemented as a view against a hidden table named `__apply_changes_storage_customers_silver`.
# MAGIC 
# MAGIC We can see this if we run `DESCRIBE EXTENDED`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC If we query this hidden table, we'll see these 3 fields. However, users shouldn't need to interact directly with this table as it's just leveraged by DLT to ensure that updates are applied in the correct order to materialize results correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM __apply_changes_storage_customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examining Data Files
# MAGIC 
# MAGIC Run the following cell to look at the files in the configured **Storage location**.

# COMMAND ----------

files = dbutils.fs.ls(f"{userhome}/output")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC The `autoloader` and `checkpoint` directories contain data used to manage incremental data processing with Structured Streaming.
# MAGIC 
# MAGIC The `system` directory captures events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls(f"{userhome}/output/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC These event logs are stored as a Delta table. Let's query the table.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{userhome}/output/system/events`"))

# COMMAND ----------

# MAGIC %md
# MAGIC We'll dive deeper into the metrics in the notebook that follows.
# MAGIC 
# MAGIC Let's view the contents of the `tables` directory.

# COMMAND ----------

files = dbutils.fs.ls(f"{userhome}/output/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Each of these directories contains a Delta Lake table being managed by DLT.
