# Databricks notebook source
import re

dbutils.widgets.text("course", "dlt")
course_name = dbutils.widgets.get("course")

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/dbacademy/{username}/{course_name}"
database = f"""da_{re.sub("[^a-zA-Z0-9]", "_", username)}_{course_name}"""

spark.conf.set("da.database", database)

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

dbutils.widgets.text("mode", "setup")
mode = dbutils.widgets.get("mode")

if mode == "reset" or mode == "clean":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)

# COMMAND ----------

data_source = userhome + "/source"

# placeholder with temp data until finalized
dbutils.fs.cp("wasbs://courseware@dbacademy.blob.core.windows.net/temp-dstrodtman-dlt-ga/v01", data_source, True)

data_landing_location = userhome + "/raw"
storage_location = userhome + "/output"

class FileArrival:
    def __init__(self, source, landing_location, batches=30):
        self.source = source
        self.landing_location = landing_location + "/"
        self.max_batch = batches
        try:
            self.batch = 1 + int(max([x[1].split(".")[0] for x in dbutils.fs.ls(self.userdir)]))
        except:
            self.batch = 0
    
    def new_data(self, continuous=False):
        if self.batch > self.max_batch:
            print("Data source exhausted\n")
        elif continuous == True:
            while self.batch <= self.max_batch:
                self.move_file("customers")
                self.move_file("orders")
                self.move_file("status")
                self.batch += 1
        else:
            self.move_file("customers")
            self.move_file("orders")
            self.move_file("status")
            self.batch += 1
            
    def move_file(self, dataset):
        curr_file = f"/{dataset}/{self.batch:02}.json"
        dbutils.fs.cp(self.source + curr_file, self.landing_location + curr_file)

File = FileArrival(data_source, data_landing_location)
