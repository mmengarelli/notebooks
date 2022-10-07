// Databricks notebook source
// DBTITLE 1,Generate tables
val numSales = 1000000000
val numItems = 30000000
sql("create database if not exists skew_db")
sql("use skew_db")

// item with id 100 is in 80% of all sales
spark.range(numSales).selectExpr(
  s"case when rand() < 0.8 then 100 else cast(rand() * $numItems as int) end as s_item_id",
  s"cast(rand() * 100 as int) as s_quantity",
  s"cast(now() as int) - cast(rand() * 360 as int) * 3600 * 24 as s_date"
)
.write.mode("overwrite").saveAsTable("skew_db.sales")

spark.range(numItems).selectExpr(
  s"id as i_item_id",
  s"cast(rand() * 1000 as int) as i_price"
)
.write.mode("overwrite").saveAsTable("skew_db.items")


// COMMAND ----------

// DBTITLE 1,Example query (SQL)
// MAGIC %sql
// MAGIC select s_date, sum(s_quantity * i_price) as total_sales
// MAGIC from sales, items where i_item_id=s_item_id
// MAGIC group by s_date
// MAGIC order by total_sales desc

// COMMAND ----------

// DBTITLE 1,Example query (Scala)
import org.apache.spark.sql.functions._
val query = spark.table("sales")
  .join(spark.table("items"), $"i_item_id" === $"s_item_id")
  .groupBy($"s_date")
  .agg(sum($"s_quantity" * $"i_price") as "total_sales")
  .orderBy($"total_sales" desc)

display(query)

// COMMAND ----------

// DBTITLE 1,Query with Skew hint (SQL)
// MAGIC %sql
// MAGIC explain
// MAGIC select s_date, sum(s_quantity * i_price) as total_sales
// MAGIC from sales, items where i_item_id=s_item_id
// MAGIC group by s_date
// MAGIC order by total_sales desc

// COMMAND ----------

// DBTITLE 1,Query with Skew hint (Scala)
val query = spark.table("sales").hint("skew")
  .join(spark.table("items"), $"i_item_id" === $"s_item_id")
  .groupBy($"s_date")
  .agg(sum($"s_quantity" * $"i_price") as "total_sales")
  .orderBy($"total_sales" desc)

display(query)
