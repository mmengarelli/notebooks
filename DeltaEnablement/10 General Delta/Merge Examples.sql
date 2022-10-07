-- Databricks notebook source
MERGE INTO goldTable t 
USING silverTable_latest_version s ON s.Country = t.Country
WHEN MATCHED AND s._change_type='update_postimage' 
  THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
WHEN NOT MATCHED 
  THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

-- COMMAND ----------

merge into gold g
using changes chgs
on g.id = chgs.id
when matched and chgs._change_type = 'update_postimage' 
  then update set 
    g.seq = chgs.seq,
    g.name = chgs.name,
    g.state = chgs.state
when matched and chgs._change_type = 'delete' 
  then delete 
when not matched
  then insert *;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC sql("drop table if exists mikem.merge")
-- MAGIC sql("drop table  if exists mikem.vw_merge")
-- MAGIC 
-- MAGIC cols = ["id", "year", "month", "value"]
-- MAGIC 
-- MAGIC rows = [
-- MAGIC   (1,2022,1,"value1"),
-- MAGIC   (2,2022,1,"value2"),
-- MAGIC   (3,2022,2,"value3"),
-- MAGIC   (4,2022,2,"value4"),
-- MAGIC   (5,2022,3,"value5"),
-- MAGIC   (6,2022,3,"value6"),
-- MAGIC   (7,2022,4,"value7"),
-- MAGIC   (8,2022,4,"value8"),
-- MAGIC   (9,2022,5,"value9"),
-- MAGIC   (10,2022,6,"value10"),
-- MAGIC   (11,2021,11,"value11"),
-- MAGIC   (12,2021,12,"value12")
-- MAGIC ]
-- MAGIC 
-- MAGIC spark.createDataFrame(rows,cols).write.partitionBy("year", "month").saveAsTable("mikem.merge")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC cols = ["id", "year", "month", "value"]
-- MAGIC 
-- MAGIC rows = [
-- MAGIC   (1,2022,1,"value1changed"),
-- MAGIC   (2,2022,1,"value2changed"),
-- MAGIC   (3,2022,2,"value3changed"),
-- MAGIC   (4,2022,2,"value4changed"),
-- MAGIC   (5,2022,3,"value5changed"),
-- MAGIC   (13,2021,10,"value13")
-- MAGIC ]
-- MAGIC 
-- MAGIC spark.createDataFrame(rows,cols).write.saveAsTable("mikem.vw_merge")

-- COMMAND ----------

--explain
merge into mikem.merge m
using mikem.vw_merge vw
on m.id = vw.id
and m.year = vw.year
and m.month = vw.month
when matched then update set * 
when not matched then insert * 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select 
-- MAGIC operationMetrics.numSourceRows,
-- MAGIC round(operationMetrics.executionTimeMs/1000,0) as execTimeS,
-- MAGIC round(operationMetrics.scanTimeMs/1000,0) as scanTimeS,
-- MAGIC round(operationMetrics.rewriteTimeMs/1000,0) as rewriteTimeS,
-- MAGIC operationMetrics.numTargetRowsInserted as rowsInserted,
-- MAGIC operationMetrics.numTargetRowsUpdated as rowsUpdated,
-- MAGIC operationMetrics.numTargetRowsDeleted as rowsDeleted,
-- MAGIC operationMetrics.numTargetFilesAdded as filesAdded
-- MAGIC from (
-- MAGIC   desc history mikem.merge 
-- MAGIC ) where operation='MERGE'

-- COMMAND ----------

optimize mikem.merge
--where month < 6
zorder by id

-- COMMAND ----------

desc history mikem.merge
