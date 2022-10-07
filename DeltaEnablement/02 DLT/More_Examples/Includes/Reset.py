# Databricks notebook source
# INCLUDE_HEADER_TRUE
# INCLUDE_FOOTER_TRUE

# COMMAND ----------

# MAGIC %md
# MAGIC Resets the environment removing any directories and or tables created during course execution

# COMMAND ----------

import re

course = "dlt"
username = spark.sql("select current_user()").first()[0]
clean_username = re.sub(r"[^a-zA-Z0-9]", "_", username)

user_db_prefix = f"da_{clean_username}_{course}"
rows = spark.sql("SHOW DATABASES").collect()
for row in rows:
    db_name = row[0]
    if db_name.startswith(user_db_prefix):
        print(f"Dropping database {db_name}")
        spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

working_dir = f"dbfs:/dbacademy/{username}/{course}"
result = dbutils.fs.rm(working_dir, True)
print(f"Deleted {working_dir}: {result}")

print("Course environment succesfully reset")
