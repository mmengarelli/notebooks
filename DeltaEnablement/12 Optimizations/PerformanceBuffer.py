# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

sc.setJobDescription("preformance test")
spark.conf.set("spark.job.description", "per_test_job")

sc.setLocalProperty("callSite.short", "stage short")
sc.setLocalProperty("callSite.long", "stage long")

sc.defaultParallelism

# COMMAND ----------

spark.conf.set("spark.job.description", "Spark Range")

df = spark.range(1, 1000).toDF("id")
display(df)

# COMMAND ----------

spark.conf.set("spark.job.description", "repartition test")

df = spark.range(1, 2).toDF("id").repartition(8)
df.count()
print(f"num partitions: {df.rdd.getNumPartitions()}")

# COMMAND ----------

print(f"num partitions: {df.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md dropDuplicates() vs distinct() vs collectSet()

# COMMAND ----------

spark.conf.set("spark.job.description", "union all")

df = spark.range(1, 2000).toDF("id").coalesce(1)
df = df.unionAll(df).unionAll(df)
df.count()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

df.drop_duplicates(["id"]).count()

# COMMAND ----------

df.agg(explode(collect_set('id'))).count()

# COMMAND ----------

# DBTITLE 1,collect_set vs count(distinct)
df.createOrReplaceTempView('df')
sql("select explode(collect_set('id')) from df").count()

# COMMAND ----------

# MAGIC %md Range Partitioner vs Hash Partitioner

# COMMAND ----------

dfp = spark.range(1, 2000).toDF("id")
dfp.count()

# COMMAND ----------

# DBTITLE 1,RoundRobinPartitioning
dfp.repartition(25).explain()

# COMMAND ----------

# DBTITLE 1,hashpartitioning
df.repartition(25, 'id').explain()

# COMMAND ----------

# MAGIC %md Creating a stage boundary
# MAGIC * Write out to disk and re-read in
# MAGIC * Write a checkpoint

# COMMAND ----------

dfs = spark.range(1, 2000).toDF('id')
dfs.localCheckpoint().explain()

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", 2)


# COMMAND ----------

spark.range(1,10000).distinct().explain()

# COMMAND ----------

df1 = spark.range(1, 1000).toDF('id')
df2 = spark.range(1, 1000).toDF('id')
df3 = df1.join(df2, 'id').explain()

# COMMAND ----------

df1.groupBy('id').sum('id').explain()

# COMMAND ----------

df1.distinct().explain()

# COMMAND ----------

# MAGIC %md # Skew
# MAGIC * group by 
# MAGIC * join
# MAGIC 
# MAGIC `withColumn("salt", salt)`

# COMMAND ----------

# MAGIC %md # TPC-DS

# COMMAND ----------

# MAGIC %run "/Users/michael.mengarelli@databricks.com/TPC-DS/TPC-benchmarking (stand-alone)/TPC-queries"

# COMMAND ----------

# MAGIC %sql 
# MAGIC use tpcds_sf1000_delta_mm;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("tpcds.query", TPCDSQueries3TB("q4"))

# COMMAND ----------

shuffle_parts = sc.defaultParallelism * 3
 
spark.conf.set("spark.sql.files.maxPartitionBytes", 268435456) # controls how many parts on load
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 5242880000)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# Shuffle
spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)

# spark.conf.set("spark.sql.adaptive.enabled", True)
# spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
# spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", shuffle_parts)
# spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 268435456)

# COMMAND ----------

q = spark.conf.get("tpcds.query")
sql(q).explain()

# COMMAND ----------

display(sql(q))

# COMMAND ----------

df = spark.range(0, 10 * 1000 * 1000) \
  .withColumn('id', (col('id') / 10000).cast('integer')) \
  .withColumn('y', rand()) \
  .withColumn('x1', rand()) \
  .withColumn('x2', rand()) \

(0 to 10).toDF
spark.createDataFrame()
val df = Seq(("a", 100)).toDF("part", "id")
val df = List(1,2,3,4).toDF("id")
val df = Seq((1,"test"),(2,"test"),(3,"test"),(4,"test")).toDF("id", "val")



# COMMAND ----------




# COMMAND ----------

import random 
list = []

for i in range(100000):
  list.append(("LA", random.random()))

for i in range(250):
  list.append(("CHI", random.random()))

for i in range(50):
  list.append(("DC", random.random()))

columns = ["city","riders"]

skew_df = sc.parallelize(list).toDF(columns)
display(skew_df)

# COMMAND ----------

counts = skew_df.groupBy('city').count()
counts.show()

# COMMAND ----------


