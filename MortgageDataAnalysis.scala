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

// COMMAND ----------

// DBTITLE 1,Run setup notebook
// MAGIC %run ./Setup

// COMMAND ----------

// DBTITLE 1,Mortgage data set: 43 Files ~ 2.4 GB  
// MAGIC %fs ls /mnt/mikem/mortgage_data

// COMMAND ----------

// DBTITLE 1,Load mortgage data into a view
val df = spark.read.option("header", "true")
  .option("delimiter", "\t")
  .option("inferSchema", "true") 
  .csv("/mnt/mikem/mortgage_data")  

df.createOrReplaceTempView("mortgage_data")
df.printSchema

// COMMAND ----------

// DBTITLE 1,Quick exploration
display(df)

// COMMAND ----------

// DBTITLE 1,Explore unpaid balance
display(df.select("unpaid_balance").describe())

// COMMAND ----------

// DBTITLE 1,Sample of unpaid balance
display(df.select("unpaid_balance").limit(50))

// COMMAND ----------

// DBTITLE 1,Avg unpaid balance by year
// MAGIC %sql SELECT year, avg(unpaid_balance) FROM mortgage_data GROUP BY year ORDER BY year asc

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

// DBTITLE 1,Predict unpaid balance with machine learning
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

// DBTITLE 1,Compare unpaid balance with prediction
// MAGIC %sql SELECT unpaid_balance, prediction FROM model_results order by unpaid_balance desc

// COMMAND ----------

// DBTITLE 1,Let's check our accuracy (using ggplot)
// MAGIC %python
// MAGIC from ggplot import *
// MAGIC pydf = (
// MAGIC   result.select("record", "unpaid_balance", "prediction")
// MAGIC     .orderBy("record",asc=False)
// MAGIC     .sample(False, 0.0001, 42)
// MAGIC     .toPandas()
// MAGIC )
// MAGIC display(
// MAGIC   ggplot(pydf, aes('record','unpaid_balance')) +
// MAGIC     geom_point(color='blue') + 
// MAGIC     geom_line(pydf, aes('record','prediction'), color='green') + 
// MAGIC     scale_x_log10() + scale_y_log10()
// MAGIC )

// COMMAND ----------

// DBTITLE 1,Identify most important features from the best model? (50%+ from annual income, 12% from zip code)
// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC importance = zip(model.stages[1].bestModel.featureImportances.toArray(), cols)
// MAGIC temp = sc.parallelize(importance).map(lambda f: [float(f[0]), f[1]])
// MAGIC display(sqlContext.createDataFrame(temp, ["importance", "feature"]).orderBy(col("importance").desc()))

// COMMAND ----------

