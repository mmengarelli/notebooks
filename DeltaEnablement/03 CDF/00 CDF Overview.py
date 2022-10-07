# Databricks notebook source
# MAGIC %md # Use cases
# MAGIC Delta change data feed is not enabled by default. The following use cases should drive when you enable the change data feed.
# MAGIC 
# MAGIC * **Silver and Gold tables**: Improve Delta performance by processing only row-level changes following initial MERGE, UPDATE, or DELETE operations to accelerate and simplify ETL and ELT operations.
# MAGIC * **Materialized views**: Create up-to-date, aggregated views of information for use in BI and analytics without having to reprocess the full underlying tables, instead updating only where changes have come through.
# MAGIC * **Transmit changes**: Send a change data feed to downstream systems such as Kafka or RDBMS that can use it to incrementally process in later stages of data pipelines.
# MAGIC * **Audit trail table**: Capture the change data feed as a Delta table provides perpetual storage and efficient query capability to see all changes over time, including when deletes occur and what updates were made.

# COMMAND ----------

# MAGIC %md # Usage
# MAGIC 
# MAGIC `ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`
