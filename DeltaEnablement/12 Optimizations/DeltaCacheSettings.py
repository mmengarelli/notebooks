# Databricks notebook source
# spark.conf.set("spark.databricks.io.cache.enabled", True)
# Set the following on the clusters
# spark.databricks.io.cache.maxDiskUsage 50g
# spark.databricks.io.cache.maxMetaDataCache 1g
# spark.databricks.io.cache.compression.enabled false

# COMMAND ----------

# MAGIC %sql 
# MAGIC use tpcds_sf1000_delta;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- select count(*) from customer, store_sales, date_dim

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 2 * sc.defaultParallelism)

# COMMAND ----------

# MAGIC %sql
# MAGIC with year_total as (
# MAGIC  select c_customer_id customer_id
# MAGIC        ,c_first_name customer_first_name
# MAGIC        ,c_last_name customer_last_name
# MAGIC        ,c_preferred_cust_flag customer_preferred_cust_flag
# MAGIC        ,c_birth_country customer_birth_country
# MAGIC        ,c_login customer_login
# MAGIC        ,c_email_address customer_email_address
# MAGIC        ,d_year dyear
# MAGIC        ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
# MAGIC        ,'s' sale_type
# MAGIC  from customer
# MAGIC      ,store_sales
# MAGIC      ,date_dim
# MAGIC  where c_customer_sk = ss_customer_sk
# MAGIC    and ss_sold_date_sk = d_date_sk
# MAGIC  group by c_customer_id
# MAGIC          ,c_first_name
# MAGIC          ,c_last_name
# MAGIC          ,c_preferred_cust_flag
# MAGIC          ,c_birth_country
# MAGIC          ,c_login
# MAGIC          ,c_email_address
# MAGIC          ,d_year
# MAGIC  union all
# MAGIC  select c_customer_id customer_id
# MAGIC        ,c_first_name customer_first_name
# MAGIC        ,c_last_name customer_last_name
# MAGIC        ,c_preferred_cust_flag customer_preferred_cust_flag
# MAGIC        ,c_birth_country customer_birth_country
# MAGIC        ,c_login customer_login
# MAGIC        ,c_email_address customer_email_address
# MAGIC        ,d_year dyear
# MAGIC        ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
# MAGIC        ,'c' sale_type
# MAGIC  from customer
# MAGIC      ,catalog_sales
# MAGIC      ,date_dim
# MAGIC  where c_customer_sk = cs_bill_customer_sk
# MAGIC    and cs_sold_date_sk = d_date_sk
# MAGIC  group by c_customer_id
# MAGIC          ,c_first_name
# MAGIC          ,c_last_name
# MAGIC          ,c_preferred_cust_flag
# MAGIC          ,c_birth_country
# MAGIC          ,c_login
# MAGIC          ,c_email_address
# MAGIC          ,d_year
# MAGIC union all
# MAGIC  select c_customer_id customer_id
# MAGIC        ,c_first_name customer_first_name
# MAGIC        ,c_last_name customer_last_name
# MAGIC        ,c_preferred_cust_flag customer_preferred_cust_flag
# MAGIC        ,c_birth_country customer_birth_country
# MAGIC        ,c_login customer_login
# MAGIC        ,c_email_address customer_email_address
# MAGIC        ,d_year dyear
# MAGIC        ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
# MAGIC        ,'w' sale_type
# MAGIC  from customer
# MAGIC      ,web_sales
# MAGIC      ,date_dim
# MAGIC  where c_customer_sk = ws_bill_customer_sk
# MAGIC    and ws_sold_date_sk = d_date_sk
# MAGIC  group by c_customer_id
# MAGIC          ,c_first_name
# MAGIC          ,c_last_name
# MAGIC          ,c_preferred_cust_flag
# MAGIC          ,c_birth_country
# MAGIC          ,c_login
# MAGIC          ,c_email_address
# MAGIC          ,d_year
# MAGIC          )
# MAGIC   select
# MAGIC                   t_s_secyear.customer_id
# MAGIC                  ,t_s_secyear.customer_first_name
# MAGIC                  ,t_s_secyear.customer_last_name
# MAGIC                  ,t_s_secyear.customer_birth_country
# MAGIC  from year_total t_s_firstyear
# MAGIC      ,year_total t_s_secyear
# MAGIC      ,year_total t_c_firstyear
# MAGIC      ,year_total t_c_secyear
# MAGIC      ,year_total t_w_firstyear
# MAGIC      ,year_total t_w_secyear
# MAGIC  where t_s_secyear.customer_id = t_s_firstyear.customer_id
# MAGIC    and t_s_firstyear.customer_id = t_c_secyear.customer_id
# MAGIC    and t_s_firstyear.customer_id = t_c_firstyear.customer_id
# MAGIC    and t_s_firstyear.customer_id = t_w_firstyear.customer_id
# MAGIC    and t_s_firstyear.customer_id = t_w_secyear.customer_id
# MAGIC    and t_s_firstyear.sale_type = 's'
# MAGIC    and t_c_firstyear.sale_type = 'c'
# MAGIC    and t_w_firstyear.sale_type = 'w'
# MAGIC    and t_s_secyear.sale_type = 's'
# MAGIC    and t_c_secyear.sale_type = 'c'
# MAGIC    and t_w_secyear.sale_type = 'w'
# MAGIC    and t_s_firstyear.dyear =  1999
# MAGIC    and t_s_secyear.dyear = 1999+1
# MAGIC    and t_c_firstyear.dyear =  1999
# MAGIC    and t_c_secyear.dyear =  1999+1
# MAGIC    and t_w_firstyear.dyear = 1999
# MAGIC    and t_w_secyear.dyear = 1999+1
# MAGIC    and t_s_firstyear.year_total > 0
# MAGIC    and t_c_firstyear.year_total > 0
# MAGIC    and t_w_firstyear.year_total > 0
# MAGIC    and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
# MAGIC            > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
# MAGIC    and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
# MAGIC            > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
# MAGIC  order by t_s_secyear.customer_id
# MAGIC          ,t_s_secyear.customer_first_name
# MAGIC          ,t_s_secyear.customer_last_name
# MAGIC          ,t_s_secyear.customer_birth_country

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC with customer_total_return as
# MAGIC (select sr_customer_sk as ctr_customer_sk
# MAGIC ,sr_store_sk as ctr_store_sk
# MAGIC ,sum(SR_FEE) as ctr_total_return
# MAGIC from store_returns
# MAGIC ,date_dim
# MAGIC where sr_returned_date_sk = d_date_sk
# MAGIC and d_year =2000
# MAGIC group by sr_customer_sk
# MAGIC ,sr_store_sk)
# MAGIC  select  c_customer_id
# MAGIC from customer_total_return ctr1
# MAGIC ,store
# MAGIC ,customer
# MAGIC where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
# MAGIC from customer_total_return ctr2
# MAGIC where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
# MAGIC and s_store_sk = ctr1.ctr_store_sk
# MAGIC and s_state = 'MO'
# MAGIC and ctr1.ctr_customer_sk = c_customer_sk
# MAGIC order by c_customer_id
# MAGIC limit 100

# COMMAND ----------


