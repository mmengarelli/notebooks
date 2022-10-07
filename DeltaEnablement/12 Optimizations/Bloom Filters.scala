// Databricks notebook source
// MAGIC %md # Bloom Filters
// MAGIC 
// MAGIC A Bloom filter index is a space-efficient data structure that enables data skipping on chosen columns, particularly for fields containing arbitrary text.
// MAGIC 
// MAGIC 
// MAGIC How Bloom filter indexes work<br>
// MAGIC The Bloom filter operates by either stating that data is definitively not in the file, or that it is probably in the file, with a defined false positive probability (FPP).
// MAGIC 
// MAGIC Way to check if an element exists in a set. Fast lookup of data 
// MAGIC 
// MAGIC Databricks supports file level Bloom filters; each data file can have a single Bloom filter index file associated with it. Before reading a file Databricks checks the index file and the file is read only if the index indicates that the file might match a data filter. Databricks always reads the data file if an index does not exist or if a Bloom filter is not defined for a queried column.
// MAGIC 
// MAGIC The size of a Bloom filter depends on the number elements in the set for which the Bloom filter has been created and the required FPP. The lower the FPP, the higher the number of used bits per element and the more accurate it will be, at the cost of more disk space and slower downloads. For example, an FPP of 10% requires 5 bits per element.
// MAGIC 
// MAGIC A Bloom filter index is `an uncompressed Parquet file that contains a single row`. Indexes are stored in the `_delta_index` subdirectory relative to the data file and use the same name as the data file with the suffix `index.v1.parquet`. For example, the index for data file `dbfs:/db1/data.0001.parquet.snappy` would be named `dbfs:/db1/_delta_index/data.0001.parquet.snappy.index.v1.parquet`.
// MAGIC 
// MAGIC Bloom filters support columns with the following (input) data types: byte, short, int, long, float, double, date, timestamp, and string. 
// MAGIC 
// MAGIC Databricks supports the following data source filters: 
// MAGIC * and
// MAGIC * or 
// MAGIC * in 
// MAGIC * equals
// MAGIC * equalsnullsafe
// MAGIC 
// MAGIC Note: Bloom filters are not supported on nested columns.

// COMMAND ----------

// MAGIC %md # Creating a bloom filter
// MAGIC 
// MAGIC `create bloom filter index on table bloom_test for columns(sha OPTIONS (fpp=0.1, numItems=50000000))`
// MAGIC 
// MAGIC Creates a Bloom filter index for **new or rewritten data**; it does not create Bloom filters for existing data.
// MAGIC 
// MAGIC While it is not possible to build a Bloom filter index for data that is already written, the `OPTIMIZE command updates Bloom filters for data that is reorganized`. Therefore, you can backfill a Bloom filter by running OPTIMIZE on a table:
// MAGIC * If you have not previously optimized the table.
// MAGIC * With a different file size, requiring that the data files be re-written.
// MAGIC * With a ZORDER (or a different ZORDER, if one is already present), requiring that the data files be re-written.
// MAGIC 
// MAGIC You can tune the Bloom filter by defining options at the column level or at the table level:
// MAGIC * `fpp`: False positive probability. The desired false positive rate per written Bloom filter. This influences the number of bits needed to put a single item in the Bloom filter and influences the size of the Bloom filter. The value must be larger than 0 and smaller than or equal to 1. The default value is 0.1 which requires 5 bits per item.
// MAGIC * `numItems`: Number of distinct items the file can contain. This setting is important for the quality of filtering as it influences the total number of bits used in the Bloom filter (number of items - number of bits per item). If this setting is incorrect, the Bloom filter is either very sparsely populated, wasting disk space and slowing queries that must download this file, or it is too full and is less accurate (higher FPP). The value must be larger than 0. The default is 1 million items.
// MAGIC * `maxExpectedFpp`: The expected FPP threshold for which a Bloom filter is not written to disk. The maximum expected false positive probability at which a Bloom filter is written. If the expected FPP is larger than this threshold, the Bloom filter’s selectivity is too low; the time and resources it takes to use the Bloom filter outweighs its usefulness. The value must be between 0 and 1. The default is 1.0 (disabled).
// MAGIC 
// MAGIC These options play a role only when writing the data. You can configure these properties at various hierarchical levels: write operation, table level, and column level. The column level takes precedence over the table and operation levels, and the table level takes precedence over the operation level.
