-- Databricks notebook source
-- DBTITLE 1,Enable the Bloom filter index capability
SET spark.databricks.io.skipping.bloomFilter.enabled = true;

-- COMMAND ----------

-- DBTITLE 1,Create table
CREATE OR REPLACE TABLE bloom_test (
  id   BIGINT NOT NULL,
  str1 STRING NOT NULL,
  sha  STRING NOT NULL,
  sha1 STRING NOT NULL,
  sha2_256 STRING NOT NULL,
  row_hash_too_big STRING NOT NULL,
  row_hash STRING NOT NULL
)
USING DELTA
LOCATION 'dbfs:/tmp/bloom_test'

-- COMMAND ----------

-- DBTITLE 1,Create Index before adding data
CREATE BLOOMFILTER INDEX
ON TABLE bloom_test
FOR COLUMNS(sha OPTIONS (fpp=0.1, numItems=50000000))

-- COMMAND ----------

-- DBTITLE 1,Generate data
TRUNCATE TABLE bloom_test;

WITH sample (
  SELECT
    id,
    'windows.exe' as str1,
    monotonically_increasing_id() mono_id,
    hash(id) hash,
    sha (cast(id % 50000000 as string)) sha,
    sha1(cast(id % 50000000 as string)) sha1,
    sha2(cast(id as string), 256)    sha2_256
  from
    RANGE(0, 1000000000, 1, 448)  -- start, end, step, numPartitions
)
INSERT INTO bloom_test 
SELECT id, 
  str1, 
  sha,
  sha1,
  sha2_256,
  sha2(concat_ws('||',id, str1, mono_id, hash, sha, sha1, sha2_256),512) row_hash_too_big,
  sha2(concat_ws('||',id, str1, mono_id, hash, sha, sha1, sha2_256),256) row_hash
FROM sample
-- LIMIT 20000

-- COMMAND ----------

SET spark.databricks.delta.optimize.maxFileSize = 1610612736;
OPTIMIZE bloom_test
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md ## Examine the physical table and index

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/bloom_test

-- COMMAND ----------

describe extended bloom_test

-- COMMAND ----------

-- MAGIC %md ## Run test queries

-- COMMAND ----------

-- DBTITLE 1,Find hashes not likely to be in same file
SELECT * FROM bloom_test WHERE id in ( 0, 1, 999999998, 999999999)

-- COMMAND ----------

-- DBTITLE 1,Query a non-BLOOMFILTER indexed column
SELECT count(*) FROM bloom_test WHERE sha1 = 'b2f9544427aed7b712b209fffc756c001712b7ca'

-- COMMAND ----------

-- DBTITLE 1,Query a BLOOMFILTER indexed column
SELECT count(*) FROM bloom_test WHERE sha = 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410c'

-- COMMAND ----------

-- DBTITLE 1,Search Bloom filter for something that isn't there
SELECT count(*) FROM bloom_test WHERE sha = 'b6589fc6ab0dc82cf12099d1c2d40ab994e8410_'

-- COMMAND ----------

-- MAGIC %md ## Results
-- MAGIC 
-- MAGIC ### Performance
-- MAGIC | Metric | Value |
-- MAGIC | --- | --- |
-- MAGIC | No Bloom filter | ~21 s |
-- MAGIC | With Bloom filter | ~13 s |
-- MAGIC | With Bloom filter on item not in set | ~9 s|
-- MAGIC 
-- MAGIC ### Cluster Configuration:
-- MAGIC 
-- MAGIC | Metric | Value|
-- MAGIC | --- | --- |
-- MAGIC | Number of workers | 4 |
-- MAGIC | Worker |16 GB, 8 core  |
-- MAGIC | Driver | 16 GB, 8 core |
-- MAGIC | Databricks Runtime | 7.1.x-scala2.12 |
-- MAGIC     
-- MAGIC ### Data:
-- MAGIC | Metric | Value  |
-- MAGIC |---|---|
-- MAGIC | Number of records |1,000,000,000  | 
-- MAGIC | Instances of the 'needle' | 20 | 
-- MAGIC | Delta Parquet data files | 140  |
-- MAGIC | Delta Parquet index files  |140 |
-- MAGIC | Data file size | 1.5GB | 
-- MAGIC | Delta index file size | 30MB| 
-- MAGIC | Total bytes | 150943777871, ~151GB |
-- MAGIC 
-- MAGIC 
-- MAGIC ### Bloom filter parameters
-- MAGIC | Parameter | Value |
-- MAGIC | --- | --- |
-- MAGIC | FPP | 0.1, 0.1% |
-- MAGIC | numItems |  50,000,000 - 50k distinct items|
