# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the Delta Live Tables UI
# MAGIC 
# MAGIC This demo will explore the DLT UI. By the end of this lesson you will be able to: 
# MAGIC 
# MAGIC * Deploy a DLT pipeline
# MAGIC * Explore the resultant DAG
# MAGIC * Execute an update of the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Setup
# MAGIC 
# MAGIC The following cell is configured to reset this demo.

# COMMAND ----------

# MAGIC %run ./Includes/setup $mode="reset"

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the following cell to print out two values that will be used during the following configuration steps.

# COMMAND ----------

print(f"source : {data_landing_location.split(':')[1]}")
print(f"Target: {database}")
print(f"Storage location: {storage_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create and configure a pipeline
# MAGIC 
# MAGIC In this section you will create a pipeline using a notebook provided with the courseware. We'll explore the contents of the notebook in the following lesson.
# MAGIC 
# MAGIC 1. Click the **Jobs** button on the sidebar,
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Make sure the **Product Edition** is set to **Advanced**.
# MAGIC 1. Fill in a **Pipeline Name** of your choosing.
# MAGIC    * Make sure this is easy to remember and unique in your workspace.
# MAGIC 1. For **Notebook Libraries**, click the file folder to open the navigator UI, then:
# MAGIC    * Navigate to either the **_Python** or **_SQL** directory
# MAGIC    * Select notebook **1_Customers_Pipeline**
# MAGIC      * Though this document is a standard Databricks Notebook, the syntax is specialized to DLT table declarations. We will be exploring the syntax in the exercise that follows.
# MAGIC 1. Under **Configuration**, click **Add configuration**.
# MAGIC    * Add the word `source` to the **Key** field. In the **Value** field, copy and paste the path printed out next to **source** in the cell above. (This should follow the pattern **`/dbacademy/<username>/dlt/raw`**)
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above. (This should follow the pattern **`da_<username>_dlt`**)
# MAGIC    * This field is optional; if not specified, then tables will not be registered to a metastore, but will still be available in the DBFS. Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">documentation</a> for more information on this option.
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC    * This optional field allows the user to specify a location to store logs, tables, and other information related to pipeline execution. If not specified, DLT will automatically generate a directory.
# MAGIC 1. For **Pipeline Mode**, select **Triggered**
# MAGIC    * This field specifies how the pipeline will be run.
# MAGIC    * **Triggered** pipelines run once and then shut down until the next manual or scheduled update.
# MAGIC    * **Continuous** pipelines run continuously, ingesting new data as it arrives. Choose the mode based on latency and cost requirements.
# MAGIC 1. Uncheck the **Enable autoscaling** box, and set the number of workers to 1.,
# MAGIC    * **Enable autoscaling**, **Min Workers** and **Max Workers** control the worker configuration for the underlying cluster processing the pipeline. Notice the DBU estimate provided, similar to that provided when configuring interactive clusters.
# MAGIC 1. Click **Create**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land a batch of data
# MAGIC 
# MAGIC The following helper function will land a new set of JSON files in the directory configured as the data source.
# MAGIC 
# MAGIC Note that this functionality has also been duplicated in the notebook `Land New Data`.

# COMMAND ----------

File.new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a pipeline
# MAGIC 
# MAGIC With a pipeline created, you will now run the pipeline.
# MAGIC 
# MAGIC 1. Select **Development** to run the pipeline in development mode. Development mode provides for more expeditious iterative development by reusing the cluster (as opposed to creating a new cluster for each run) and disabling retries so that you can readily identify and fix errors. Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentation</a> for more information on this feature.
# MAGIC 2. Click **Start**.
# MAGIC 
# MAGIC The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring the DAG
# MAGIC 
# MAGIC As the pipeline completes, the execution flow is graphed. 
# MAGIC 
# MAGIC Selecting the tables reviews the details.
# MAGIC 
# MAGIC Select **orders_silver**. Notice the results reported in the **Data Quality** section. 
# MAGIC 
# MAGIC With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land another batch of data
# MAGIC 
# MAGIC Run the cell below to land more data in the source directory, then manually trigger a pipeline update.

# COMMAND ----------

File.new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC As we continue through the course, you can return to this notebook and use the method provided above to land new data.
# MAGIC 
# MAGIC Running this entire notebook again will delete the underlying data files for both the source data and your DLT Pipeline. If you get disconnected from your cluster or have some other event where you wish to land more data without deleting things, refer to the notebook titled `Land New Data`.
