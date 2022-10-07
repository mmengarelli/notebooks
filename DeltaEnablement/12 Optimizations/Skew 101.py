# Databricks notebook source
# MAGIC %md 
# MAGIC # Overview
# MAGIC Data skew is caused by transformations that change data partitioning like join, groupBy, and orderBy. For example, joining on a key that is not evenly distributed across the cluster, causing some partitions to be very large and not allowing Spark to process data in parallel. Since this is a well-known problem, there is a bunch of available solutions for it. In this article, I will share my experience of handling data skewness in Apache Spark.
# MAGIC 
# MAGIC Clearly, we can see one long-running task with a massive shuffle read. After taking a closer look at this long-running task, we can see that it processed almost 50% of the input(see Shuffle Read Records column).
# MAGIC 
# MAGIC Sort Merge Joins send all records with the same join key to the same partition, and it looks like almost 50% of our input rows have the same value in the join column.
# MAGIC   
# MAGIC A shuffle causes the data to be repartitioned. A good partition will minimize the amount of data movement needed by the program. 
# MAGIC 
# MAGIC The most common place where you encounter skews is when you perform join operations or group by operations on your datasets. When you perform these operations, all the “related” records are directed to the same node for processing. This might sometimes result in most of the records being directed to a single node and forcing only a single node to handle all the data. 
# MAGIC 
# MAGIC Same values or null in join or group by - all go to same executor
# MAGIC 
# MAGIC Spark maps a key to a particular partition id by computing a hash code on the key and dividing it by the number of shuffle partitions.
# MAGIC 
# MAGIC ## Issues
# MAGIC * Slow-Running Stages/Tasks: certain operations will take very long because a given worker is working with too much data.
# MAGIC * Spilling Data to Disk: if data does not fit in memory on a worker, it will be written to disk which takes much longer.
# MAGIC * Out of Memory errors: if worker runs out of disk space, an error is thrown.
# MAGIC 
# MAGIC ## Joins
# MAGIC * Join column was highly skewed
# MAGIC * Nulls in join column<br>
# MAGIC Spark hashes the join column and sorts it. It then tries to keep the records with the same hashes in both partitions on the same executor so that all the null values of the table go to one executor, and Spark gets into a continuous loop of shuffling and garbage collection with no success. If there are too many null values in a join or group-by key they would skew the operation. Try to preprocess the null values with some random ids and handle them in the application.
# MAGIC 
# MAGIC ## Identifying
# MAGIC * After the query finishes, find a stage that does a join and check task duration distribution
# MAGIC * Sort the tasks by duration decreasing and check the first few tasks.
# MAGIC * In this example, Task 5 took 6.7min while the next longest task took only 1s.
# MAGIC * Additionally, Task 5 read 7.6GB from Shuffle while the next read only 14MB. The Shuffle imbalance is a result of skew.
# MAGIC 
# MAGIC Run a count grouped by partition col
# MAGIC 
# MAGIC `import pyspark.sql.functions as F`<br>
# MAGIC `df.groupBy(F.spark_partition_id()).count().show()`
# MAGIC 
# MAGIC 
# MAGIC ## Remedies
# MAGIC * Avoid joins on skewed values by isolating them.
# MAGIC * Broadcast Hash join is a reasonable solution as long as the datasets are not too large. If you are dealing with extremely large datasets, then you can salt the datasets to increase parallelism and hence performance.
# MAGIC * Re-partition your data based on the transformations in your script. In short, if you’re grouping or joining, partitioning by the groupBy/join columns can improve shuffle efficiency
# MAGIC 
# MAGIC `df = df.repartition(m, col1, col2, col3)`
# MAGIC 
# MAGIC ### AQE
# MAGIC In Databricks Runtime 7.3 LTS and above, AQE is enabled by default. It has 4 major features:
# MAGIC * Dynamically changes **join strategy**: sort merge join into broadcast hash join.
# MAGIC * Dynamically **coalesces partitions** (combine small partitions into reasonably sized partitions) after shuffle exchange. Very small tasks have worse I/O throughput and tend to suffer more from scheduling overhead and task setup overhead. Combining small tasks saves resources and improves cluster throughput.
# MAGIC * Dynamically handles **skew** in sort merge join and shuffle hash join by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks.
# MAGIC * Dynamically detects and propagates empty relations.
# MAGIC 
# MAGIC `spark.conf.set(“spark.sql.adaptive.enabled”, true)`
# MAGIC 
# MAGIC 
# MAGIC ### Key Salting
# MAGIC One approach to avoiding a data skew is to “salt” your datasets and then include the salt in the join condition.
# MAGIC * Add a new key which guarantees an even distribution of data
# MAGIC * Add one column with uniformly distributed values to the big data frame
# MAGIC * Add one column to the small data frame
# MAGIC * Explode the small data frame, meaning we create new rows for each ‘old_id’ and each n in [0, salt] range.
# MAGIC 
# MAGIC `import pyspark.sql.functions as F`<br>
# MAGIC `df = df.withColumn('salt', F.rand())`<br>
# MAGIC `df = df.repartition(8, 'salt')`
# MAGIC 
# MAGIC Key salting involves the following steps:
# MAGIC 
# MAGIC A range of salt values is first selected. Using larger salt values allow for the datasets to be more distributed, but it also increases the size of the explosion 
# MAGIC The lookup table is exploded with a new record generated for each salt value.
# MAGIC If there are 5 salt values, then for each record in the lookup table, 5 records are generated.
# MAGIC Each of these 5 records will only differ based on the salt value.
# MAGIC The range of salt values is then evenly distributed in the base table as an added column.
# MAGIC Finally, a join is performed on the join key as well as the salt. Since salt is part of the join key, it forces the data to be distributed across partitions not only on the basis of join keys but also based on salt values.
# MAGIC 
# MAGIC ## Reference
# MAGIC * https://www.clairvoyant.ai/blog/optimizing-the-skew-in-spark
# MAGIC * https://towardsdatascience.com/data-skew-in-pyspark-783d529a9dd7
# MAGIC * https://itnext.io/handling-data-skew-in-apache-spark-9f56343e58e8
# MAGIC * https://selectfrom.dev/data-skew-in-apache-spark-f5eb194a7e2
# MAGIC * https://selectfrom.dev/spark-performance-tuning-skewness-part-2-9f50c765a87e
# MAGIC * https://www.unraveldata.com/common-failures-slowdowns-part-ii/
# MAGIC * [Skew Join Optimization Guide](https://databricks.atlassian.net/wiki/spaces/UN/pages/40311744/Skew+Join+Optimization+Guide)

# COMMAND ----------

# MAGIC %md # Usage
# MAGIC To test this use a small vm (4 cores or less) and many instances 10 or more to see proper skew

# COMMAND ----------

sql("drop database if exists skew_db cascade")
sql("create database if not exists skew_db")

# COMMAND ----------

# MAGIC %scala
# MAGIC val numItems = 30000000
# MAGIC 
# MAGIC // item with id 100 is in 90% of all sales
# MAGIC spark.range(1000000000).selectExpr(
# MAGIC   s"case when rand() < 0.9 then 100 else cast(rand() * $numItems as int) end as item_id",
# MAGIC   s"cast(rand() * 100 as int) as s_quantity",
# MAGIC   s"cast(now() as int) - cast(rand() * 360 as int) * 3600 * 24 as s_date"
# MAGIC ).write.mode("overwrite").saveAsTable("sales")
# MAGIC 
# MAGIC spark.range(numItems).selectExpr(
# MAGIC   s"id as item_id",
# MAGIC   s"cast(rand() * 1000 as int) as i_price"
# MAGIC ).write.mode("overwrite").saveAsTable("items")

# COMMAND ----------

# MAGIC %md # Start Here
# MAGIC 
# MAGIC 
# MAGIC See [this](https://docs.google.com/document/d/1fZ1Y8fJJqPCLX_PguPFezDD7qjgHWc-JRe146Yt34es/edit#)

# COMMAND ----------

# Turning this off so we can see skewed sort-merge join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 5242880000)

# spark.conf.set("spark.sql.files.maxPartitionBytes", 268435456) # controls how many parts on load
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

# Shuffle
shuffle_parts = sc.defaultParallelism * 3
# spark.conf.set("spark.sql.shuffle.partitions", "auto")
# spark.conf.set("spark.sql.shuffle.partitions", shuffle_parts)
# spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", shuffle_parts)
# spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
# spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", 268435456)

# COMMAND ----------

print(spark.conf.get("spark.sql.adaptive.enabled"))
sql("use skew_db")

# COMMAND ----------

from pyspark.sql.functions import *

sales = table("sales")
sales.count()
#display(sales.groupBy("item_id").count().orderBy(col("count").desc()))

# COMMAND ----------

#display(sales.groupBy(spark_partition_id()).count())

# COMMAND ----------

items = table("items")
items.count()

# COMMAND ----------

joined = sales.join(items, "item_id")
display(joined)

# COMMAND ----------

# MAGIC %md # Step 1 Try repartitioning on the skewed table
# MAGIC 
# MAGIC Let's repartition the sales DF 
# MAGIC 
# MAGIC You should now see the following for the 1 stage in the longest running job:
# MAGIC * Low variance in task distribution
# MAGIC * Low variance in GC time across tasks
# MAGIC * Low variance in input size + shuffle write time

# COMMAND ----------

# Repartition and materialize
sales_repart = sales.repartition(sc.defaultParallelism)
sales_repart.count()

# COMMAND ----------

display(sales_repart.join(items, "item_id"))

# COMMAND ----------

# MAGIC %md-sandbox # Step 2 - Key Salting
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/1400/1*MX0t_dg580KGuseFATG-kQ.png" style="width: 550px;" />
# MAGIC 
# MAGIC [Image Source](https://selectfrom.dev/data-skew-in-apache-spark-f5eb194a7e2)
# MAGIC 
# MAGIC The main table will have one of the n salt values<br>
# MAGIC The lookup table is exploded with a new record for each salt value<br>
# MAGIC The join will happen on whatever salt value the main table has = 1 of the n salt values the lookup table has

# COMMAND ----------

# Salt Range: this specifies how much the data will be distributed. 
# While greater number of salts increases the distribution, it also increases the explosion of lookup table
# Hence a balance is necessary.
n = 5
# produces salt values = [0, 1, ... n-1]  

salt_values = list(range(n)) 

# explode the lookup dataframe with salt
# This will generage n records for each record where n = len(salt_values)
items = items.withColumn("salt_values", array([lit(i) for i in salt_values]))
items = items.withColumn("i_salt", explode(items.salt_values)).drop("salt_values")

# distribute salt evently in the base table
sales = sales.withColumn("s_salt", monotonically_increasing_id() % n)

# COMMAND ----------

print(sales.count())
print(items.count())

# COMMAND ----------

join_cond = (sales.item_id == items.item_id) & (sales.s_salt == items.i_salt)
joined = sales.join(items, join_cond)

display(joined)

# COMMAND ----------

# MAGIC %md Now look at the Spark UI. 
# MAGIC 
# MAGIC You should see pretty even distribution of task time, gc time and input/ouput data size.

# COMMAND ----------

# MAGIC %md # Step 3 - Try Skew-Join Hint
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC https://docs.databricks.com/delta/join-performance/skew-join.html#skew-join-optimization

# COMMAND ----------

# same can be done in DF API
sales.createOrReplaceTempView("vw_sales")
items.createOrReplaceTempView("vw_items")

sales_vw = table("vw_sales")
print(sales_vw.count())

items_vw = table("vw_items")
print(items_vw.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select /*+ SKEW('vw_sales', 'item_id') */ * from vw_sales s
# MAGIC inner join vw_items i 
# MAGIC on s.item_id = i.item_id
