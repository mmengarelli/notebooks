// Databricks notebook source
// MAGIC %md-sandbox ## Mortgage Data Analysis 
// MAGIC <img src="https://www.fhfa.gov/PublishingImages/logo.png" style="height: 90px; margin: 10px; border: 1px solid #ddd; border-radius: 10px 10px 10px 10px; padding: 5px"/>
// MAGIC #### Workflow
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/workflow1.png" width="40%"/>
// MAGIC #### Datasources
// MAGIC <li>Amazon S3 Files
// MAGIC <li>Spark SQL Database
// MAGIC <li>Redshift Database
// MAGIC <small><p>_This notebook uses [anonymized mortgage information](https://www.fhfa.gov/DataTools/Downloads/Pages/Public-Use-Databases.aspx) from Fannie Mae and Freddie Mac._ </small>
// MAGIC   
// MAGIC <!--
// MAGIC # demo-ready
// MAGIC # mm-demo
// MAGIC # Mortgage Demo
// MAGIC -->

// COMMAND ----------

// DBTITLE 1,Setup
// MAGIC %run ./Setup

// COMMAND ----------

// MAGIC %md #### Explore

// COMMAND ----------

// DBTITLE 1,Mortgage data set: 43 Files ~ 2.4 GB  
// MAGIC %fs ls /mnt/mikem/mortgage_data

// COMMAND ----------

// DBTITLE 1,Load data
val df = spark.read.option("header", "true")
  .option("delimiter", "\t")
  .option("inferSchema", "true") 
  .csv("/mnt/mikem/mortgage_data")  

df.createOrReplaceTempView("mortgage_data")

// COMMAND ----------

df.printSchema

// COMMAND ----------

// DBTITLE 0,Quick exploration
display(df)

// COMMAND ----------

// DBTITLE 1,Explore unpaid balance
display(df.select("unpaid_balance").describe())

// COMMAND ----------

// DBTITLE 1,Avg unpaid balance by year
// MAGIC %sql SELECT year, avg(unpaid_balance) FROM mortgage_data GROUP BY year ORDER BY year asc

// COMMAND ----------

// MAGIC %md #### Data Engineering

// COMMAND ----------

// DBTITLE 1,Load geolocation data (Redshift)
val geo = spark.read.format("com.databricks.spark.redshift")
  .option("url", jdbcUri)
  .option("dbtable", "fips") // Fips/State Code Mappings
  .option("tempdir", "s3n://mikem-rs-temp")
  .option("user", rsUser)
  .option("password", rsPass)
  .option("forward_spark_s3_credentials", "true")
  .load()

geo.cache.count

// COMMAND ----------

// DBTITLE 1,Federation: join mortgage + geolocation
val mortgages = spark.read.table("mortgage_data")
val joined = mortgages.join(geo, mortgages("usps_code") === geo("fips_code"))

joined.createOrReplaceTempView("mortgages_with_geo")
display(joined.select("record","usps_code", "state_name", "state_code"))

// COMMAND ----------

// DBTITLE 1,Unpaid balance by state
// MAGIC %sql select state_code, avg(unpaid_balance) from mortgages_with_geo group by state_code

// COMMAND ----------

// MAGIC %md #### Train

// COMMAND ----------

// DBTITLE 1,Train DecisionTreeRegressor
// MAGIC %python
// MAGIC sfhDf = spark.read.table("mortgage_data")
// MAGIC 
// MAGIC from pyspark.ml.regression import *
// MAGIC from pyspark.ml.feature import *
// MAGIC from pyspark.ml import *
// MAGIC from pyspark.ml.tuning import *
// MAGIC from pyspark.ml.evaluation import *
// MAGIC 
// MAGIC cols = sfhDf.columns
// MAGIC cols.remove('unpaid_balance')
// MAGIC 
// MAGIC va = VectorAssembler(inputCols=cols, outputCol = 'features')
// MAGIC dt = DecisionTreeRegressor(maxDepth=10, varianceCol="variance", labelCol='unpaid_balance')
// MAGIC re = RegressionEvaluator(predictionCol="prediction", labelCol="unpaid_balance", metricName="mae")
// MAGIC tvs = TrainValidationSplit(estimator=dt, evaluator=re, seed=42, estimatorParamMaps=ParamGridBuilder().addGrid(dt.maxDepth, [10]).build())
// MAGIC 
// MAGIC pipeline = Pipeline(stages = [va, tvs])
// MAGIC 
// MAGIC model = pipeline.fit(sfhDf)
// MAGIC result = model.transform(sfhDf)
// MAGIC 
// MAGIC result.registerTempTable('model_results')

// COMMAND ----------

// DBTITLE 1,What were our features?
// MAGIC %python 
// MAGIC for (a,b) in enumerate(zip(range(0, len(cols)), cols)):
// MAGIC   print (b)

// COMMAND ----------

// MAGIC %md #### Accuracy

// COMMAND ----------

// DBTITLE 0,Compare unpaid balance with prediction
// MAGIC %sql select unpaid_balance, prediction from model_results order by unpaid_balance desc

// COMMAND ----------

// MAGIC %python
// MAGIC import plotly.express as px
// MAGIC 
// MAGIC # to Pandas
// MAGIC pds = result.select("unpaid_balance", "prediction") \
// MAGIC  .sample(False, 0.0001, 42) \
// MAGIC  .toPandas()
// MAGIC 
// MAGIC plt = px.scatter(pds, title="Prediction vs Unpaid Balance", \
// MAGIC         x="prediction", y="unpaid_balance", \
// MAGIC         log_x=True, log_y=True, \
// MAGIC         trendline="lowess", trendline_color_override="#8FBC8F", \
// MAGIC         width=640, height=480)
// MAGIC 
// MAGIC plt.show()