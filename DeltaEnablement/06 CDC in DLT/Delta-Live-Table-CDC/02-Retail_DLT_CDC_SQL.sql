-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
-- MAGIC 
-- MAGIC ## Importance of Change Data Capture (CDC)
-- MAGIC 
-- MAGIC Change Data Capture (CDC) is the process that captures the changes in records made to transactional Database (Mysql, Postgre) or Data Warehouse. CDC captures operations like data deletion, append and updating, typically as a stream to re-materialize the table in external systems.
-- MAGIC 
-- MAGIC CDC enables incremental loading while eliminating the need for bulk load updating.
-- MAGIC 
-- MAGIC By capturing CDC events, we can re-materialize the source table as Delta Table in our Lakehouse and start running Analysis on top of it (Data Science, BI), merging the data with external system.
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_cdc%2Fnotebook_dlt_sql&dt=DLT_CDC">
-- MAGIC <!-- [metadata={"description":"Process CDC from external system and save them as a Delta Table. BRONZE/SILVER.<br/><i>Usage: demo CDC flow.</i>",
-- MAGIC  "authors":["mojgan.mazouchi@databricks.com"],
-- MAGIC  "db_resources":{},
-- MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into", "cdc", "cdf"]},
-- MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Capturing CDC  
-- MAGIC 
-- MAGIC A variety of **CDC tools** are available. One of the open source leader solution is Debezium, but other implementation exists simplifying the datasource, such as Fivetran, Qlik Replicate, Streamset, Talend, Oracle GoldenGate, AWS DMS.
-- MAGIC 
-- MAGIC In this demo we are using CDC data coming from an external system like Debezium or DMS. 
-- MAGIC 
-- MAGIC Debezium takes care of capturing every changed row. It typically sends the history of data changes to Kafka logs or save them as file. To simplify the demo, we'll consider that our external CDC system is up and running and saving the CDC as JSON file in our blob storage (S3, ADLS, GCS). 
-- MAGIC 
-- MAGIC Our job is to CDC informations from the `customer` table (json format), making sure they're correct, and then materializing the customer table in our Lakehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Materializing table from CDC events with Delta Live Table
-- MAGIC 
-- MAGIC In this example, we'll synchronize data from the Customers table in our MySQL database.
-- MAGIC 
-- MAGIC - We extract the changes from our transactional database using Debezium or any other tool and save them in a cloud object storage (S3 folder, ADLS, GCS).
-- MAGIC - Using Autoloader we incrementally load the messages from cloud object storage, and stores the raw messages them in the `customers_cdc`. Autoloader will take care of infering the schema and handling schema evolution for us.
-- MAGIC - Then we'll add a view `customers_cdc_clean` to check the quality of our data, using expectation, and then build dashboards to track data quality. As example the ID should never be null as we'll use it to run our upsert operations.
-- MAGIC - Finally we perform the APPLY CHANGES INTO (doing the upserts) on the cleaned cdc data to apply the changes to the final `customers` table
-- MAGIC 
-- MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt_pipeline.png" width="1000"/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### CDC input from tools like Debezium
-- MAGIC 
-- MAGIC For each change, we receive a JSON message containing all the fields of the row being updated (customer name, email, address...). In addition, we have extra metadata informations including:
-- MAGIC 
-- MAGIC - operation: an operation code, typically (DELETE, APPEND, UPDATE)
-- MAGIC - operation_date: the date and timestamp for the record came for each operation action
-- MAGIC 
-- MAGIC Tools like Debezium can produce more advanced output such as the row value before the change, but we'll exclude them for the clarity of the demo

-- COMMAND ----------

-- DBTITLE 1,Input data from CDC
-- MAGIC %python
-- MAGIC display(spark.read.json("/tmp/demo/cdc_raw/customers"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC #### Ingesting data with Autoloader
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt_pipeline_1.png" width="600" style="float: right" />
-- MAGIC 
-- MAGIC Our first step is to ingest the data from the cloud storage. Again, this could be from any other source like (message queue etc).
-- MAGIC 
-- MAGIC This can be challenging for multiple reason. We have to:
-- MAGIC 
-- MAGIC - operate at scale, potentially ingesting millions of small files
-- MAGIC - infer schema and json type
-- MAGIC - handle bad record with incorrect json schema
-- MAGIC - take care of schema evolution (ex: new column in the customer table)
-- MAGIC 
-- MAGIC Databricks Autoloader solves all these challenges out of the box.

-- COMMAND ----------

-- DBTITLE 1,Let's ingest our incoming data using Autoloader (cloudFiles)
CREATE STREAMING LIVE TABLE customers_cdc 
COMMENT "New customer data incrementally ingested from cloud object storage landing zone"
AS SELECT * FROM cloud_files("/tmp/demo/cdc_raw/customers", "json", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC #### Adding expectations to track data quality
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt_pipeline_2.png" width="600" style="float: right" />
-- MAGIC 
-- MAGIC Next, we'll add expectations to controle data quality. To do so, we'll create a view (we don't need to duplicate the data) and check the following conditions:
-- MAGIC 
-- MAGIC - ID must never be null
-- MAGIC - the cdc operation type must be valid
-- MAGIC - the json must have been properly read by the autoloader
-- MAGIC 
-- MAGIC If one of these conditions isn't respected, we'll drop the row.
-- MAGIC 
-- MAGIC 
-- MAGIC These expectations metrics are saved as technical tables and can then be re-used with Databricks SQL to track data quality over time.

-- COMMAND ----------

-- DBTITLE 1,Silver Layer - Cleansed Table (Impose Constraints)
CREATE STREAMING LIVE VIEW customers_cdc_clean(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_operation EXPECT (operation IN ('APPEND', 'DELETE', 'UPDATE')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_json_schema EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleansed cdc data, tracking data quality with a view. We ensude valid JSON, id and operation type"
AS SELECT * FROM STREAM(live.customers_cdc);

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Materializing the silver table with APPLY CHANGE
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/cdc_dlt_pipeline_3.png" width="600" style="float: right" />
-- MAGIC 
-- MAGIC The silver `customer` table will contains the most up to date view. It'll be a replicate of the original table.
-- MAGIC 
-- MAGIC This is non trivial to implement manually. You need to consider things like data deduplication to keep the most recent row.
-- MAGIC 
-- MAGIC Thanksfully Delta Live Table solve theses challenges out of the box with the `APPLY CHANGE` operation

-- COMMAND ----------

-- DBTITLE 1,Create the target customers table 
CREATE INCREMENTAL LIVE TABLE customers
  COMMENT "Clean, materialized customers";

-- COMMAND ----------

APPLY CHANGES INTO live.customers
FROM stream(live.customers_cdc_clean)
  KEYS (id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date 
  COLUMNS * EXCEPT (operation, operation_date, _rescued_data);  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Conclusion 
-- MAGIC We now have our pipeline up & ready! Our `customers` table is materialize and we can start building BI report to analyze and improve our business. It also open the door to Data Science and ML use-cases such as customer churn, segmentation etc.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC #### Monitoring your data quality metrics with Delta Live Table
-- MAGIC 
-- MAGIC <img style="float:right" width="500" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC 
-- MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
-- MAGIC 
-- MAGIC This let you build custom dashboards to track those metrics.
-- MAGIC 
-- MAGIC 
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard</a>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### How to run this pipeline
-- MAGIC 
-- MAGIC To create and run the DLT pipeline, add a path to this notebook and **add configuration with enabling applychanges to true**. For more detail see notebook "PipelineSettingConfiguration.json". 
-- MAGIC 
-- MAGIC You also need to add the companion generator notebook.
-- MAGIC 
-- MAGIC After running the pipeline, check "3. Retail_DLT_CDC_Monitoring" to monitor log events and lineage data.
