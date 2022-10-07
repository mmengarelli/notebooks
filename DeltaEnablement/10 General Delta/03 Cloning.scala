// Databricks notebook source
// MAGIC %md 
// MAGIC Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.
// MAGIC 
// MAGIC The metadata that is cloned includes: schema, partitioning information, invariants, nullability. 
// MAGIC 
// MAGIC For deep clones only, stream and COPY INTO metadata are also cloned. 
// MAGIC 
// MAGIC You can use CLONE for complex operations like data migration, data archiving, machine learning flow reproduction, short-term experiments, data sharing etc. See [https://docs.databricks.com/delta/delta-utility.html#clone-use-cases](Clone use cases) for a few examples.
// MAGIC 
// MAGIC A deep clone is a clone that copies the source table data to the clone target in addition to the metadata of the existing table. Additionally, stream metadata is also cloned such that a stream that writes to the Delta table can be stopped on a source table and continued on the target of a clone from where it left off.
// MAGIC 
// MAGIC A shallow clone is a clone that does not copy the data files to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create.

// COMMAND ----------

OPTIMIZE always creates new files. So if you run it on the source table it create new files there and the cloned one won't see them at all. If you run it on the cloned table it'll create new files there and the source table won't see them at all.

So yes, doing shallow clone + OPTIMIZE is a great way to explore different data layouts relatively cheaply

