# Databricks notebook source
# MAGIC %fs mkdirs /mnt/mikem/tmp/datagen

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
import string, random

def id_generator(size, chars=string.ascii_uppercase + string.digits):
  return ''.join(random.choice(chars) for _ in range(size))

def string_generator():
  return''.join(random.choices(string.ascii_uppercase + string.digits, k=100))

# COMMAND ----------

n_rows = 1000*1000*1000
spec = dg.DataGenerator(spark, rows=n_rows)
spec = spec.withIdOutput()

for x in range(1000):
  spec = spec.withColumn(f"col{x}", StringType(), values=[string_generator()])

df = spec.build()

# COMMAND ----------

df.repartition(n_rows / sc.defaultParallelism / 10000)
df.count()

# COMMAND ----------

# MAGIC %fs rm -r /mnt/mikem/tmp/datagen