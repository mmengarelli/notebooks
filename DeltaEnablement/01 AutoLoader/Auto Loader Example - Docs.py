# Databricks notebook source
user_dir = 'michael.mengarelli@databricks.com'
upload_path = "/FileStore/shared-uploads/" + user_dir + "/population_data_upload"

dbutils.fs.mkdirs(upload_path)
print(f"upload_path: {upload_path}")

# COMMAND ----------

wa_csv = "city,year,population\nSeattle metro,2019,3406000\nSeattle metro,2020,3433000"
or_csv = "city,year,population\nPortland metro,2019,2127000\nPortland metro,2020,2151000"

dbutils.fs.put(f"{upload_path}/wa.csv", wa_csv)
dbutils.fs.put(f"{upload_path}/or.csv", or_csv)

# COMMAND ----------

display(dbutils.fs.ls(upload_path))

# COMMAND ----------

checkpoint_path = '/tmp/delta/population_data/_checkpoints'
write_path = '/tmp/delta/population_data'

df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .schema('city string, year int, population long') \
  .load(upload_path)

df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# COMMAND ----------

display(spark.read.format('delta').load(write_path))

# COMMAND ----------

id_csv = """city,year,population
Boise,2019,438000
Boise,2020,447000"""

mt_csv="""city,year,population
Helena,2019,81653
Helena,2020,82590"""

misc_csv = """city,year,population
Seattle metro,2021,3461000
Portland metro,2021,2174000
Boise,2021,455000
Helena,2021,81653"""

dbutils.fs.put(f"{upload_path}/id.csv", id_csv)
dbutils.fs.put(f"{upload_path}/mt.csv", mt_csv)
dbutils.fs.put(f"{upload_path}/misc.csv", misc_csv)

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

dbutils.fs.rm(write_path, True)
dbutils.fs.rm(upload_path, True)
