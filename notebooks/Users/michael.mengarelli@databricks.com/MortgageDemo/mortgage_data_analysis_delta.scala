// Databricks notebook source
// MAGIC %md-sandbox ## Mortgage Data Analysis 
// MAGIC <img src="https://www.fhfa.gov/PublishingImages/logo.png" style="height: 90px; margin: 10px; border: 1px solid #ddd; border-radius: 10px 10px 10px 10px; padding: 5px"/>
// MAGIC #### Workflow
// MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/workflow1.png" width="40%"/>
// MAGIC 
// MAGIC <small><p>_This notebook uses [anonymized mortgage information](https://www.fhfa.gov/DataTools/Downloads/Pages/Public-Use-Databases.aspx) from Fannie Mae and Freddie Mac._ </small>
// MAGIC   
// MAGIC <!--
// MAGIC # demo-ready
// MAGIC # mm-demo
// MAGIC # Mortgage Demo
// MAGIC -->

// COMMAND ----------

// DBTITLE 0,Setup
// MAGIC %run ./setup

// COMMAND ----------

// MAGIC %md #### Explore

// COMMAND ----------

// DBTITLE 1,Mortgage data set: 43 Files ~ 2.4 GB  
// MAGIC %fs ls /mnt/mikem/mortgage_data

// COMMAND ----------

// DBTITLE 1,Load data
val df = spark.read.option("header", "true")
  .option("delimiter", "\t")
  .schema(schema)
  .csv("/mnt/mikem/mortgage_data")  
  .withColumnRenamed("usps_code", "fips_code")

df.createOrReplaceTempView("mortgage_data")

// COMMAND ----------

df.printSchema

// COMMAND ----------

// DBTITLE 0,Quick exploration
display(df)

// COMMAND ----------

// MAGIC %md #### Data Engineering

// COMMAND ----------

// DBTITLE 0,Join state data
// join state code table
val fips = table("mikem.st_code") 
val joined = df.join(fips, Seq("fips_code"))

display(joined)

// COMMAND ----------

joined.repartition(16).write.mode("overwrite")
 .format("delta")
 .saveAsTable("mikem.mortgage_delta")

// COMMAND ----------

// MAGIC %md #### Explore our new Delta table

// COMMAND ----------

// DBTITLE 1,Unpaid balance by state
// MAGIC %sql select st_code, avg(unpaid_balance) from mikem.mortgage_delta group by st_code

// COMMAND ----------

// DBTITLE 1,Avg unpaid balance by year
// MAGIC %sql SELECT year, avg(unpaid_balance) FROM mortgage_data GROUP BY year ORDER BY year asc

// COMMAND ----------

// DBTITLE 1,Explore unpaid balance
val mtgDelta = table("mikem.mortgage_delta")
display(mtgDelta.select("unpaid_balance").describe())

// COMMAND ----------

// MAGIC %md #### Predict unpaid balance

// COMMAND ----------

val features = Array(
 "census_median_income",
 "local_median_income",
 "tract_income_ratio",
 "borrowers_annual_income",
 "borrower_income_ratio",
 "loan_purpose",
 "federal_guarantee",
 "number_of_borrowers",
 "first_time_buyers",
 "borrower_race_1",
 "borrower_ethnicity",
 "co_borrower_race_1",
 "co_borrower_ethnicity",
 "borrower_gender",
 "co_borrower_gender",
 "borrower_age",
 "co_borrower_age",
 "property_type",
 "year"
)

// COMMAND ----------

// DBTITLE 1,Train
val indexer = new VectorAssembler()
  .setInputCols(features)
  .setOutputCol("indexedFeatures")

val dt = new DecisionTreeRegressor()
  .setLabelCol("unpaid_balance")
  .setFeaturesCol("indexedFeatures")

val Array(train, test) = mtgDelta.randomSplit(Array(.7, .3))

val pipeline = new Pipeline().setStages(Array(indexer, dt))
val model = pipeline.fit(train)

// COMMAND ----------

// DBTITLE 1,Predict
val results = model.transform(test)
results.createOrReplaceTempView("results")

// COMMAND ----------

// MAGIC %python
// MAGIC import plotly.express as px
// MAGIC 
// MAGIC # to Pandas
// MAGIC pds = table("results").select("unpaid_balance", "prediction") \
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