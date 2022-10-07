-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
-- MAGIC 
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_cdc%2Fnotebook_readme&dt=DLT_CDC">
-- MAGIC <!-- [metadata={"description":"Process CDC from external system and save them as a Delta Table. BRONZE/SILVER.<br/><i>Usage: demo CDC flow.</i>",
-- MAGIC  "authors":["mojgan.mazouchi@databricks.com"],
-- MAGIC  "db_resources":{},
-- MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into", "cdc", "cdf"]},
-- MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

-- COMMAND ----------

displayHTML('''<iframe
 src="https://docs.google.com/presentation/d/10Dmx43aZXzfK9LJvJjH1Bjgwa3uvS2Pk7gVzxhr3H2Q/embed?slide=id.p9&rm=minimal"
  frameborder="0"
  width="75%"
  height="600"
></iframe>''')
