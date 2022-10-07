# Databricks notebook source
# MAGIC %sql
# MAGIC show databases like 'tpcds*';
# MAGIC use tpcds_sf1000_delta;
# MAGIC show tables;

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
# MAGIC limit 100

# COMMAND ----------

sql1 = """
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
"""

sql2 = """
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
       ,'c' sale_type
 from customer
     ,catalog_sales
     ,date_dim
 where c_customer_sk = cs_bill_customer_sk
   and cs_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
"""

sql3 = """
select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
         )
  select
                  t_s_secyear.customer_id
                 ,t_s_secyear.customer_first_name
                 ,t_s_secyear.customer_last_name
                 ,t_s_secyear.customer_birth_country
 from year_total t_s_firstyear
     ,year_total t_s_secyear
     ,year_total t_c_firstyear
     ,year_total t_c_secyear
     ,year_total t_w_firstyear
     ,year_total t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
   and t_s_firstyear.customer_id = t_c_secyear.customer_id
   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_secyear.customer_id
   and t_s_firstyear.sale_type = 's'
   and t_c_firstyear.sale_type = 'c'
   and t_w_firstyear.sale_type = 'w'
   and t_s_secyear.sale_type = 's'
   and t_c_secyear.sale_type = 'c'
   and t_w_secyear.sale_type = 'w'
   and t_s_firstyear.dyear =  1999
   and t_s_secyear.dyear = 1999+1
   and t_c_firstyear.dyear =  1999
   and t_c_secyear.dyear =  1999+1
   and t_w_firstyear.dyear = 1999
   and t_w_secyear.dyear = 1999+1
   and t_s_firstyear.year_total > 0
   and t_c_firstyear.year_total > 0
   and t_w_firstyear.year_total > 0
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
 order by t_s_secyear.customer_id
         ,t_s_secyear.customer_first_name
         ,t_s_secyear.customer_last_name
         ,t_s_secyear.customer_birth_country
"""

# COMMAND ----------

df1 = sql(sql1)
df1.count()



# df2 = sql(sql2)
# df2.count()


#df3 = sql(sql3)

# COMMAND ----------

df1

# COMMAND ----------

from pyspark.sql.functions import * 

#df1.rdd.getNumPartitions()
ct = df1.groupBy(spark_partition_id()).count()
display(ct)

# COMMAND ----------

df = df1.groupBy("customer_id").count().alias("ct")
display(ct)

# COMMAND ----------

event = spark.read.parquet('...').select(['event_id', 'invoice_id', 'session'])
invoice = spark.read.parquet('...').select(['invoice_id', 'sale_amt'])

# Salt Range
# this specifies how much the data will be distributed. 
# While greater number of salts increases the distribution, it also increases the explosion of lookup table
# Hence a balance is necessary.
n = 5
salt_values = list(range(n))     # i.e salt values = [0, 1, ..... 4]                             

# explode the lookup dataframe with salt
# This will generage n records for each record where n = len(salt_values)
invoice = invoice.withColumn("salt_values", array([lit(i) for i in salt_values]))
invoice = invoice.withColumn("_salt_", explode(invoice.salt_values)).drop("salt_values")

# distribute salt evently in the base table
event = event.withColumn("_salt_", monotonically_increasing_id() % n)

#  SaltedJoin
join_condition = [event.invoice_id == invoice.invoice_id, event._salt_ == invoice._salt_]
df_joined = event.join(invoice, join_condition, 'left').drop("_salt_")
