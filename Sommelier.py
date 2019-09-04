# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC # Using Collaborative Filtering to recommend wine
# MAGIC Choosing a wine can be a lot like the game of roulette. There are countless varities and flavor profiles, making selection resemble guesswork for the average wine drinker. Many think that price is an indicator of quality, but that is not always the case. 
# MAGIC 
# MAGIC Besides the grape variety (which there are over ten thousand), environmental factors like the climate and PH of the soil can have significant impact on the profile of the wine.  
# MAGIC 
# MAGIC Many fine restaurants employ **sommeliers** to guide diners in their selection process. Sommeliers have extensive knowledge in all of the characteristics that contrinute to a wine's profile. Their role is to make accurate recommendations based on your past experiences. 
# MAGIC 
# MAGIC Similar to an experienced sommelier, we will use machine learning to make predictions based on the experiences and preferences of wine drinkers with a similar profile.
# MAGIC 
# MAGIC Our data set, scraped from [Wine Enthusiast](https://www.winemag.com/) contains the following fields:
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/winesplash.jpeg" style="float:right; height: 250px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
# MAGIC 
# MAGIC * country 
# MAGIC * description
# MAGIC * designation
# MAGIC * points - number of points Wine Enthusiast rated the wine on a scale of 1-100
# MAGIC * price
# MAGIC * province
# MAGIC * region_1
# MAGIC * taster_name
# MAGIC * title (name)
# MAGIC * variety
# MAGIC * winery
# MAGIC 
# MAGIC <small>This experiment was largely motivated by this [project](https://www.kaggle.com/sudhirnl7/wine-recommender).</small>

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql.types import *
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from wordcloud import WordCloud,STOPWORDS
import matplotlib.pyplot as plt

schema = StructType([
  StructField("country", StringType(), True),
  StructField("description", StringType(), True),
  StructField("designation", StringType(), True),
  StructField("points", IntegerType(), True),
  StructField("price", IntegerType(), True),
  StructField("province", StringType(), True),
  StructField("region_1", StringType(), True),
  StructField("region_2", StringType(), True),
  StructField("taster_name", StringType(), True),
  StructField("taster_twitter_handle", StringType(), True),
  StructField("title", StringType(), True),
  StructField("variety", StringType(), True),
  StructField("winery", StringType(), True)])
print('Done')

# COMMAND ----------

# MAGIC %md ###Data engineering

# COMMAND ----------

# Read and drop any with null points
df = spark.read.schema(schema).csv("/mnt/mikem/wine-mag/csv") \
.na.drop("all", subset=["points"])

# Add country code data
cc = spark.table("mikem.iso_country_codes").select("name", "alpha-3").withColumnRenamed("alpha-3","cc")
df2 = df.join(cc, cc.name == df.country, "left_outer")

# Drop unused columns
ratingsDF = df2.drop("taster_twitter_handle").drop("name").drop("region_2") 
ratingsDF.createOrReplaceTempView("wine_ratings")
ratingsDF.cache().count()

display(ratingsDF.sort(desc("points")))

# COMMAND ----------

# DBTITLE 1,Count
ratingsDF.count()

# COMMAND ----------

# MAGIC %md ## Exploration

# COMMAND ----------

# DBTITLE 1,Distribution of ratings by country
# MAGIC %sql select cc, count(*) from wine_ratings group by cc 

# COMMAND ----------

# DBTITLE 1,Descriptors
# Get all descriptors and serialize to string
l = list(ratingsDF.select('description').toPandas()['description'])
st = ''.join(str(e.encode('ascii','ignore')) for e in l)

wc = WordCloud(max_words=1000, width=640, height=480, margin=0, stopwords=STOPWORDS, colormap='viridis').generate(st)
 
plt.imshow(wc, interpolation='bilinear')
plt.axis("off")
plt.margins(x=0, y=0)
display(plt.show())

# COMMAND ----------

# DBTITLE 1,Price: Top 95 percentile by variety
# MAGIC %sql 
# MAGIC SELECT
# MAGIC   variety, price
# MAGIC FROM (
# MAGIC   SELECT variety, price, 
# MAGIC     ntile(100) OVER (PARTITION BY price order by price) as percentile
# MAGIC   FROM wine_ratings) tmp
# MAGIC WHERE
# MAGIC   percentile > 95
# MAGIC   and price > 100

# COMMAND ----------

# DBTITLE 1,Price vs Points
# MAGIC %sql select points, price from wine_ratings order by price desc limit 100

# COMMAND ----------

# DBTITLE 1,Correlation?
print ratingsDF.stat.corr("points","price")

# COMMAND ----------

# MAGIC %md ## Collaborative Filtering

# COMMAND ----------

# DBTITLE 1,Train 
(train,test) = ratingsDF.randomSplit([0.8, 0.2], seed = 12345)
print("Counts: train {} test {}".format(train.cache().count(), test.cache().count()))

userIndexer = StringIndexer(inputCol="taster_name", outputCol="user_id", handleInvalid="skip")
titleIndexer = StringIndexer(inputCol="title", outputCol="item_id", handleInvalid="skip")

als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="item_id", ratingCol="points", coldStartStrategy="drop", nonnegative=True)

pipeline = Pipeline()
pipeline.setStages([userIndexer, titleIndexer, als])

model = pipeline.fit(train)

# COMMAND ----------

# DBTITLE 0,Predict
predictions = model.transform(test)
predictions.createOrReplaceTempView("predictions")

# COMMAND ----------

# MAGIC %md ## Recommendations

# COMMAND ----------

display(predictions.filter("user_id == 12"))

# COMMAND ----------

alsModel = model.stages[2]
userRecs = alsModel.recommendForAllUsers(10)
itemRecs = alsModel.recommendForAllItems(10)

# COMMAND ----------

# DBTITLE 1,User recommendations
df = userRecs.filter("user_id == 12").selectExpr("explode(recommendations.item_id)")
display(df)

# COMMAND ----------

# MAGIC %md ###Evaluation
# MAGIC We evaluate the recommendation by measuring the Mean Squared Error of rating prediction.

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(metricName="rmse", labelCol="points", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = %f" % rmse)