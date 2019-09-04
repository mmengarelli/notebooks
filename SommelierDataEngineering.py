# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Using Databricks to Analyze Wine
# MAGIC 
# MAGIC Choosing a wine can be a lot like the game of roulette. There are countless varities and flavor profiles, making selection resemble guesswork for the average wine drinker. Many think that price is an indicator of quality, but that is not always the case. 
# MAGIC 
# MAGIC Besides the grape variety (which there are over ten thousand), environmental factors like the climate and PH of the soil can have significant impact on the profile of the wine.  
# MAGIC 
# MAGIC Many fine restaurants employ **sommeliers** to guide diners in their selection process. Sommeliers have extensive knowledge in all of the characteristics that contrinute to a wine's profile. Their role is to make accurate recommendations based on your past experiences. 
# MAGIC 
# MAGIC We will use the Databricks platform to explore a data set scraped from [Wine Enthusiast](https://www.winemag.com/) 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/winesplash.jpeg" style="float:right; height: 250px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
# MAGIC 
# MAGIC The data set contains the following fields:<small>
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

# MAGIC %md ### Step 1: Load Data

# COMMAND ----------

# MAGIC %fs ls /mnt/mikem/wine-mag/csv

# COMMAND ----------

# MAGIC %md ### Reading using the DataFrame API
# MAGIC The DataFrame API has many common data engineering and analytics tasks: `count()`, `join()`, `union()`, `dropDuplicates()`
# MAGIC 
# MAGIC To more advanced aggregations like: `cube`, `pivot`, `rollup`, and `window`
# MAGIC 
# MAGIC [DataSet API](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset)

# COMMAND ----------

# DBTITLE 1,Read csv
df = spark.read.schema(schema).csv("/mnt/mikem/wine-mag/csv") 
display(df)

# COMMAND ----------

# DBTITLE 1,Create a view 
df.createOrReplaceTempView("temp_view")

# COMMAND ----------

# DBTITLE 1,Query view from data frame
# MAGIC %scala spark.table("temp_view").printSchema

# COMMAND ----------

# DBTITLE 1,SQL
# MAGIC %sql select * from temp_view where variety = 'Merlot'

# COMMAND ----------

# DBTITLE 1,Descriptors
from wordcloud import WordCloud,STOPWORDS
import matplotlib.pyplot as plt

l = list(df.select('description').toPandas()['description'])
st = ''.join(str(e.encode('ascii','ignore')) for e in l)

wc = WordCloud(max_words=1000, width=640, height=480, margin=0, stopwords=STOPWORDS, colormap='viridis').generate(st)
 
plt.imshow(wc, interpolation='bilinear')
plt.axis("off")
plt.margins(x=0, y=0)
display(plt.show())

# COMMAND ----------

# MAGIC %md ### Step 2. Data Engineering

# COMMAND ----------

# Drop any with null points
df = df.na.drop("all", subset=["points"])

# Join country code 
cc = spark.table("mikem.iso_country_codes").select("name", "alpha-3").withColumnRenamed("alpha-3","cc")
df2 = df.join(cc, cc.name == df.country, "left_outer")

# Drop unused columns
ratingsDF = df2.drop("taster_twitter_handle").drop("name").drop("region_2") 
ratingsDF.createOrReplaceTempView("wine_ratings")

# COMMAND ----------

# MAGIC %sql desc wine_ratings

# COMMAND ----------

# DBTITLE 1,Write out parquet + sql table
spark.sql("drop table if exists mikem.wine_ratings_out")
ratingsDF.write.saveAsTable("mikem.wine_ratings_out")
ratingsDF.write.mode("overwrite").format("parquet").save("/mnt/mikem/wine-mag/parquet")

# COMMAND ----------

# DBTITLE 1,CTAS (or CTL)
# MAGIC %sql 
# MAGIC drop table if exists mikem.french_wine_ratings;
# MAGIC create table mikem.french_wine_ratings as (select * from mikem.wine_ratings where country = 'France');
# MAGIC 
# MAGIC --create table mikem.french_wine_ratings like mikem.wine_ratings;
# MAGIC --insert into mikem.french_wine_ratings (select * from mikem.wine_ratings where country = 'France');

# COMMAND ----------

# MAGIC %sql use mikem;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md ## Exploration
# MAGIC 
# MAGIC With the Databricks Workspace you can utilize the pre-canned visualizations within the platform or BYOV like `Bokeh`, `ggplot`, `matplotlib`, etc.
# MAGIC 
# MAGIC You can also connect BI tools like `Looker`, `Tableau`, `Superset`, or any JDBC or ODBC compatible tool.

# COMMAND ----------

# DBTITLE 1,Count
ratingsDF.count()

# COMMAND ----------

# DBTITLE 1,Count by country
ct = ratingsDF.groupBy("Country").count().where("Count > 5000")
display(ct)

# COMMAND ----------

# DBTITLE 1,Distribution of ratings by country
# MAGIC %sql select cc, count(*) from wine_ratings group by cc 

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

# MAGIC %md #### Statistical functions
# MAGIC 
# MAGIC The DataFrame API even has statistical functions like `corr()`, `freq()`, `stddev()`
# MAGIC 
# MAGIC [DataFrameStatFunctions](https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/DataFrameStatFunctions.html)

# COMMAND ----------

display(ratingsDF.select("price").describe())

# COMMAND ----------

# DBTITLE 1,Correlation - DF
print ratingsDF.stat.corr("points","price")

# COMMAND ----------

# DBTITLE 1,Correlation - SQL
# MAGIC %sql SELECT corr(points, price) as correlation from wine_ratings