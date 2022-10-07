# Databricks notebook source
# MAGIC %sql
# MAGIC set table_name = "mikem.employee_struct"
# MAGIC drop table if exists @table_name; 
# MAGIC 
# MAGIC create table mikem.employee_struct
# MAGIC (
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   employee_info struct<employer: string, id: BIGint, address: string>,
# MAGIC   places_lived array<struct<street: string, city: string, country: string>>,
# MAGIC   memorable_moments map<string, struct<year: int, place: string, details: string>>,
# MAGIC   current_address struct<street_address: struct<street_number: int, street_name: string, street_type: string>, country: string, postal_code: string>
# MAGIC );

# COMMAND ----------

# MAGIC %sql desc mikem.employee_struct

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/mikem.db/donuts_json/

# COMMAND ----------

# MAGIC %sh 
# MAGIC export FILE=/dbfs/user/hive/warehouse/mikem.db/donuts_json/part-00000-66e90a4c-3031-4786-9668-77b24a5de9bf-c000.snappy.parquet
# MAGIC 
# MAGIC parquet-tools inspect $FILE

# COMMAND ----------

# MAGIC %sql desc extended mikem.donuts_json

# COMMAND ----------

# MAGIC %pip install parquet-tools

# COMMAND ----------

json = """{
  "id": "0001",
  "type": "donut",
  "name": "Cake",
  "ppu": 0.55,
  "batters": {
    "batter": [
      {
        "id": "1001",
        "type": "Regular"
      },
      {
        "id": "1002",
        "type": "Chocolate"
      },
      {
        "id": "1003",
        "type": "Blueberry"
      },
      {
        "id": "1004",
        "type": "Devil's Food"
      }
    ]
  },
  "topping": [
    {
      "id": "5001",
      "type": "None"
    },
    {
      "id": "5002",
      "type": "Glazed"
    },
    {
      "id": "5005",
      "type": "Sugar"
    },
    {
      "id": "5007",
      "type": "Powdered Sugar"
    },
    {
      "id": "5006",
      "type": "Chocolate with Sprinkles"
    },
    {
      "id": "5003",
      "type": "Chocolate"
    },
    {
      "id": "5004",
      "type": "Maple"
    }
  ]
}"""

dbutils.fs.put("/mnt/mikem/data/json/donuts", json)

# COMMAND ----------

df = spark.read.option("multiline", True) \
  .json("/mnt/mikem/data/json/donuts") \
  .withColumnRenamed("topping", "toppings")

df.coalesce(1).write \
  .option("overwriteSchema", True) \
  .mode("overwrite") \
  .saveAsTable("mikem.donuts_json")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select explode(toppings.id) as toppings_id from mikem.donuts_json

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from mikem.donuts_json

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import pandas as pd

pd = pd.read_json("https://itunes.apple.com/gb/rss/customerreviews/id=1500780518/sortBy=mostRecent/json")
pd

# COMMAND ----------

df = spark.read.option("multiline", True) \
  .json("/mnt/mikem/data/json/itunes.json") \
  .drop("author")

df.coalesce(1).write.mode("overwrite") \
  .saveAsTable("mikem.itunes_json")

# COMMAND ----------

# MAGIC %sql desc mikem.itunes_json

# COMMAND ----------

# MAGIC %sql 
# MAGIC select feed from mikem.itunes_json

# COMMAND ----------

# STEP 1: RUN THIS CELL TO INSTALL BAMBOOLIB

# You can also install bamboolib on the cluster. Just talk to your cluster admin for that
%pip install bamboolib

# COMMAND ----------

# STEP 2: RUN THIS CELL TO IMPORT AND USE BAMBOOLIB

import bamboolib as bam

# This opens a UI from which you can import your data
bam  

# Already have a pandas data frame? Just display it!
# Here's an example
# import pandas as pd
# df_test = pd.DataFrame(dict(a=[1,2]))
# df_test  # <- You will see a green button above the data set if you display it

# COMMAND ----------

# STEP 1: RUN THIS CELL TO INSTALL BAMBOOLIB

# You can also install bamboolib on the cluster. Just talk to your cluster admin for that
%pip install bamboolib

# COMMAND ----------

# STEP 2: RUN THIS CELL TO IMPORT AND USE BAMBOOLIB

import bamboolib as bam

# This opens a UI from which you can import your data
bam  

# Already have a pandas data frame? Just display it!
# Here's an example
# import pandas as pd
# df_test = pd.DataFrame(dict(a=[1,2]))
# df_test  # <- You will see a green button above the data set if you display it
