# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC 
# MAGIC # Product Recommendations Using Databricks
# MAGIC 
# MAGIC <img src="https://brysmiwasb.blob.core.windows.net/demos/images/instacart_userbasedcollab.gif" style="float:left; margin-right: 35px; margin-left: 10px; height: 290px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px;">
# MAGIC 
# MAGIC Here we will use **Collaborative Filtering** to generate product recommendations. Collaborative filtering is commonly used for recommender systems. These techniques aim to fill in the missing entries of a user-item association matrix. **spark.ml** currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries. spark.ml uses the **alternating least squares (ALS) algorithm** to learn these latent factors. 
# MAGIC     
# MAGIC The standard approach to matrix factorization based collaborative filtering treats the entries in the user-item matrix as explicit preferences given by the user to the item, for example, users giving ratings to movies.  It is common in many real-world use cases to only have access to implicit feedback (e.g. views, clicks, purchases, likes, shares etc.). The approach used in spark.ml to deal with such data is taken from Collaborative Filtering for Implicit Feedback Datasets. Essentially, instead of trying to model the matrix of ratings directly, this approach treats the data as numbers representing the strength in observations of user actions (such as the number of clicks, or the cumulative duration someone spent viewing a movie). Those numbers are then related to the level of confidence in observed user preferences, rather than explicit ratings given to items. The model then tries to find latent factors that can be used to predict the expected preference of a user for an item.
# MAGIC 
# MAGIC Our data set is from [Instacart](https://www.kaggle.com/c/instacart-market-basket-analysis). It provides cart-level details on over 3 million grocery orders placed by over 200,000 Instacart users across a portfolio of nearly 50,000 products.  

# COMMAND ----------

# MAGIC %md ## Data Engineering

# COMMAND ----------

# DBTITLE 1,Idempotence
# MAGIC %sql 
# MAGIC drop database if exists mikem_instacart cascade;
# MAGIC create database mikem_instacart;
# MAGIC use mikem_instacart;

# COMMAND ----------

# DBTITLE 1,Order data
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = StructType([
  StructField('order_id', IntegerType()),
  StructField('user_id', IntegerType()),
  StructField('eval_set', StringType()),
  StructField('order_number', IntegerType()),
  StructField('order_dow', IntegerType()),
  StructField('order_hour_of_day', IntegerType()),
  StructField('days_since_prior_order', FloatType())
])

orders = spark.read.option("header","true") \
  .schema(schema) \
  .csv("/mnt/mikem/data/instacart-all/orders/orders.csv") \
  .drop("orders")

display(orders)

# COMMAND ----------

# DBTITLE 1,Product data
schema = StructType([
  StructField('product_id', IntegerType()),
  StructField('product_name', StringType()),
  StructField('aisle_id', IntegerType()),
  StructField('department_id', IntegerType())
])

products = spark.read.option("header","true") \
  .schema(schema) \
  .csv("/mnt/mikem/data/instacart-all/products/products.csv")

display(products)

# COMMAND ----------

# DBTITLE 1,Order Products
# Note: this data has been pre-split so we will union it and persist it as a Delta table

# Train Data
schema = StructType([
  StructField('order_id', IntegerType()),
  StructField('product_id', IntegerType()),
  StructField('add_to_cart_order', IntegerType()),
  StructField('reordered', IntegerType())
])

order_products_train = spark.read.option("header","true") \
  .schema(schema) \
  .csv("/mnt/mikem/data/instacart-all/order_products__prior/order_products__prior.csv") \
  .withColumn("split", lit('train'))

# Test Data
order_products_test = spark.read.option("header","true") \
  .schema(schema) \
  .csv("/mnt/mikem/data/instacart-all/order_products__train/order_products__train.csv") \
  .withColumn("split", lit('test'))

print("Test orders:", '{:,}'.format(order_products_test.count()))

order_products_all = order_products_train.union(order_products_test).join(orders, "order_id")
display(order_products_all)

# COMMAND ----------

# MAGIC %md ### Save Data 

# COMMAND ----------

products.write.format("delta").saveAsTable("mikem_instacart.products")
order_products_all.write.format("delta").saveAsTable("mikem_instacart.order_products")

# COMMAND ----------

# MAGIC %md ### Derive 'Naive' Product Ratings
# MAGIC 
# MAGIC We will use number of purchases to indicate our implicit user feedback for a product.

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace view user_ratings as (
# MAGIC   select split, user_id, product_id, sum(1) as purchases 
# MAGIC   from order_products
# MAGIC   group by split, user_id, product_id
# MAGIC );
# MAGIC 
# MAGIC select * from user_ratings;

# COMMAND ----------

# MAGIC %md ## Model Training

# COMMAND ----------

from pyspark.ml.recommendation import ALS
import mlflow
import mlflow.spark

mlflow.start_run(experiment_id=3031526246264475)

train = table("user_ratings").where(expr("split = 'train'"))
mlflow.log_param("training records", train.count())

test = table("user_ratings").where(expr("split = 'test'"))
mlflow.log_param("test records", test.count())

als = ALS(maxIter=5, \
          regParam=0.01, \
          userCol="user_id", \
          itemCol="product_id", \
          ratingCol="purchases", \
          coldStartStrategy="drop", \
          nonnegative=True)

model = als.fit(train)
mlflow.spark.log_model(model, "instacart-als1/")

# COMMAND ----------

# MAGIC %md ## Predictions

# COMMAND ----------

predictions = model.transform(test)

# COMMAND ----------

predictions = predictions.join(products.drop("aisle_id", "department_id"), "product_id").drop("product_id")
predictions.createOrReplaceTempView("predictions")

# COMMAND ----------

# DBTITLE 1,Recommendations for a specific user
# MAGIC %sql 
# MAGIC select product_name from predictions 
# MAGIC where user_id = 64
# MAGIC order by prediction desc

# COMMAND ----------

# DBTITLE 1,Generate top 10 product recommendations for each user
user_recs = model.recommendForAllUsers(10).select("user_id", explode("recommendations").alias("recommendations"))
user_recs.createOrReplaceTempView("user_recs")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select product_name from user_recs ur
# MAGIC join products p on p.product_id = ur.recommendations.product_id
# MAGIC where user_id = 5200

# COMMAND ----------

# MAGIC %md ## Evaluation

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(metricName="rmse", labelCol="purchases", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)

mlflow.log_metric("rmse", rmse)

# COMMAND ----------

mlflow.end_run()