-- Databricks notebook source
-- MAGIC %md # Window
-- MAGIC 
-- MAGIC Produces a new column like 'rank' or 'row'
-- MAGIC 
-- MAGIC `OVER` - determines windows (sets of rows) to operate on<br>
-- MAGIC `PARTITION BY` - Groupings = splits the result set into partitions on which the window function is applied. `PARTITION BY COL ORDER BY COL`
-- MAGIC 
-- MAGIC Window Functions:
-- MAGIC * `rank` = Ranks like Olympic Medals (may not be consecutive). Where two people have the same salary they are assigned the same rank. When multiple rows share the same rank the next rank in the sequence is not consecutive. This is like olympic medaling in that if two people share the gold, there is no silver medal etc.
-- MAGIC * `dense_rank` = Assigns consecutive ranks (ties are consecutive). 
-- MAGIC * `lag` The LAG function is used to access data from a previous row. The following query returns the salary from the previous row to calculate the difference between the salary of the current row and that of the previous row. Notice that the ORDER BY of the LAG function is used to order the data by salary.
-- MAGIC * `lead` 
-- MAGIC 
-- MAGIC 
-- MAGIC https://www.youtube.com/watch?v=H6OTMoXjNiM
-- MAGIC 
-- MAGIC 
-- MAGIC Compared to GROUP BY:
-- MAGIC Can select other fields without them being part of the group by
-- MAGIC Can have multiple aggregations (windows) in single query 
-- MAGIC Functions
-- MAGIC Aggregate - COUNT, SM, AVG, MIN, MAX
-- MAGIC Offset - FIRST_VALUE, LAST_VALUE, LEAD, LAG
-- MAGIC Statistical - RANK, NTILE, PERCENTILE
-- MAGIC NTILE(5) divides the window (group of rows) into 5 even groups (groups of as-equal-size-as-possible)\
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 	SQL	DataFrame API
-- MAGIC Ranking functions	rank	rank
-- MAGIC dense_rank	denseRank
-- MAGIC percent_rank	percentRank
-- MAGIC ntile	ntile
-- MAGIC row_number	rowNumber
-- MAGIC Analytic functions	cume_dist	cumeDist
-- MAGIC first_value	firstValue
-- MAGIC last_value	lastValue
-- MAGIC lag	lag
-- MAGIC lead	lead
-- MAGIC 
-- MAGIC 
-- MAGIC ### Reference
-- MAGIC [Oracle Docs on Analytic Functions](https://oracle-base.com/articles/misc/rank-dense-rank-first-last-analytic-functions)

-- COMMAND ----------

-- MAGIC %md ## Ranking

-- COMMAND ----------

-- DBTITLE 1,Create data
create or replace temp view emp(year, no, dep_no, name, sal) as
values 
    (2018, 1, 1, 'a', 100),
    (2018, 2, 1, 'b',  100),
    (2018, 3, 1, 'c',  40),
    (2018, 4, 1, 'd',  40),
    (2019, 5, 2, 'e', 120),
    (2019, 6, 2, 'f', 110),
    (2019, 7, 2, 'g',  80),
    (2019, 8, 2, 'h',  60),
    (2018, 9, 3, 'i', 105),
    (2018, 10, 3, 'j',  25),
    (2018, 11, 3, 'k',  45),
    (2018, 12, 4, 'l',  45),
    (2019, 13, 4, 'm', 125),
    (2019, 14, 4, 'n', 115),
    (2019, 15, 5, 'o',  85),
    (2019, 16, 5, 'p',  65),
    (2020, 17, 5, 'q',  999);

select * from emp;

-- COMMAND ----------

select name, dep_no, sal,
dense_rank() over (partition by dep_no order by sal desc) as rnk
from emp
where dep_no=1

-- COMMAND ----------

select name, dep_no, sal,
rank() over (partition by dep_no order by sal desc) as rnk
from emp
where dep_no = 1

-- COMMAND ----------

-- MAGIC %md ## LAG / LEAD
-- MAGIC 
-- MAGIC Show m-0-m growth

-- COMMAND ----------

select *,
 lag(sal, 1, 0) over (order by sal) as sal_prev,
 sal - LAG(sal, 1, 0) over (order by sal) as sal_diff
from emp

-- COMMAND ----------

SELECT *,
 LEAD(sal, 1, 0) OVER (ORDER BY sal) AS sal_next,
 LEAD(sal, 1, 0) OVER (ORDER BY sal) - sal AS sal_diff
FROM emp;

-- COMMAND ----------

-- MAGIC %md # PIVOT

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales(year, quarter, region, sales) AS
VALUES 
  (2018, 1, 'east', 100),
  (2018, 2, 'east',  20),
  (2018, 3, 'east',  40),
  (2018, 4, 'east',  40),
  (2019, 1, 'east', 120),
  (2019, 2, 'east', 110),
  (2019, 3, 'east',  80),
  (2019, 4, 'east',  60),
  (2018, 1, 'west', 105),
  (2018, 2, 'west',  25),
  (2018, 3, 'west',  45),
  (2018, 4, 'west',  45),
  (2019, 1, 'west', 125),
  (2019, 2, 'west', 115),
  (2019, 3, 'west',  85),
  (2019, 4, 'west',  65);
  
select * from sales;

-- COMMAND ----------

-- DBTITLE 1,Sum sales by quarter
select * from sales
  PIVOT (sum(sales) AS sales
    for quarter in (1,2,3,4))

-- COMMAND ----------

-- DBTITLE 1,Sum sales by region
select * from sales
  PIVOT (sum(sales) AS sales
    for region
    in ("east" as sum_sales_east_region, "west" as sum_sales_west_region))
order by year, quarter