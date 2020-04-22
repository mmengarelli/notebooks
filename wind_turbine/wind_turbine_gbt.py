# Databricks notebook source
# MAGIC %md-sandbox # Wind Turbine Predictive Maintenance with Databricks 
# MAGIC ### Vibration Analysis <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/wind_turbines.jpg" style="float:right; height: 300px; margin: 5px; padding: 5px; border: 1px solid #ddd;"/>
# MAGIC 
# MAGIC Utility-scale wind turbines have historically experienced premature component failures, which subsequently increase the cost of energy. In many cases, these failures are caused by faults in the drivetrain, led by the main gearbox. 
# MAGIC 
# MAGIC In this notebook, we demonstrate anomaly detection for the purposes of finding damaged wind turbines. A damaged, inactive wind turbine costs energy utility companies thousands of dollars per day in losses.
# MAGIC 
# MAGIC Our dataset consists of vibration readings received from sensors located in the gearboxes of wind turbines. We will use **Gradient Boosted Tree Classification** to predict which set of vibrations could be indicative of a failure.
# MAGIC <br><br>
# MAGIC <small>
# MAGIC This experiment was largely motivated by this [study](https://www.nrel.gov/docs/fy12osti/54530.pdf)<br/>
# MAGIC The [data set](https://openei.org/datasets/dataset/wind-turbine-gearbox-condition-monitoring-vibration-analysis-benchmarking-datasets) we use was provided by National Renewable Energy Laboratory (NREL)
# MAGIC </small>
# MAGIC 
# MAGIC <!--
# MAGIC mm-demo
# MAGIC demo-ready
# MAGIC -->

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Sensor locations
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/databricks-demo-images/wind_turbine/wtsmall.png" style="float:left; height: 250px; margin: 5px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px;"/>
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/databricks-demo-images/wind_turbine/wind_small.png" style="float:middle; height: 300px; margin: 5px;"/>

# COMMAND ----------

# MAGIC %run ./wind_turbine_setup

# COMMAND ----------

# MAGIC %md #### Data Exploration
# MAGIC 
# MAGIC We will explore our two `Parquet` data sets with turbine data. The data is located in a DBFS mount on S3
# MAGIC * /mnt/mcm/datasets/windturbines/healthy/
# MAGIC * /mnt/mcm/datasets/windturbines/damaged/

# COMMAND ----------

# MAGIC %fs ls /mnt/mcm/datasets/windturbines

# COMMAND ----------

healthyDF = spark.read.parquet("/mnt/mcm/datasets/windturbines/healthy/")
damagedDF = spark.read.parquet("/mnt/mcm/datasets/windturbines/damaged/")

print("Healthy Measurements: {:,}".format(healthyDF.count()))
print("Damaged Measurements: {:,}".format(damagedDF.count()))

# COMMAND ----------

# DBTITLE 1,Sample
randomSample = healthyDF.withColumn("ReadingType", lit("HEALTHY")).sample(False, 500 / 24000000.0) \
    .union(damagedDF.withColumn("ReadingType", lit("DAMAGED")).sample(False, 500 / 24000000.0))

# COMMAND ----------

display(randomSample)

# COMMAND ----------

# DBTITLE 0,Scatter plot matrix
display(randomSample)

# COMMAND ----------

# DBTITLE 0,Histogram - AN9
display(randomSample)

# COMMAND ----------

display(randomSample)

# COMMAND ----------

# MAGIC %md #### Training

# COMMAND ----------

# MAGIC %md ##### Workflows with Spark ML Pipeline
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/pub-tc/ML-workflow.png" width="550">

# COMMAND ----------

df = healthyDF.withColumn("ReadingType", lit("HEALTHY")) \
    .union(damagedDF.withColumn("ReadingType", lit("DAMAGED")))

# COMMAND ----------

# DBTITLE 1,Feature engineering
from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

featureCols = ["AN3", "AN4", "AN5", "AN6", "AN7", "AN8", "AN9", "AN10"]
va = VectorAssembler(inputCols=featureCols, outputCol="va")

stages = [va, \
          StandardScaler(inputCol="va", outputCol="features"), \
          StringIndexer(inputCol="ReadingType", outputCol="label")]

pipeline = Pipeline(stages=stages)

featurizer = pipeline.fit(df)
featurizedDf = featurizer.transform(df)

# COMMAND ----------

train, test = featurizedDf.select(["label", "features"]).randomSplit([0.7, 0.3], 42)
train = train.repartition(20)

train.cache().count()
test.cache().count()

print(train.count())

# COMMAND ----------

# DBTITLE 1,Train GBT Classifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import GBTClassifier

gbt = GBTClassifier(labelCol="label", featuresCol="features", seed=42)

grid = ParamGridBuilder().addGrid(gbt.maxDepth, [4, 5, 6]).build()

ev = BinaryClassificationEvaluator()

mlflow.set_experiment("/Users/michael.mengarelli@databricks.com/wind_turbine/wind_turbine_expirement")

with mlflow.start_run() as run:
  # 3-fold cross validation
  cv = CrossValidator(estimator=gbt, \
                    estimatorParamMaps=grid, \
                    evaluator=ev, \
                    numFolds=3)
  
  cvModel = cv.fit(train)

# COMMAND ----------

predictions = cvModel.transform(test)

mlflow.spark.log_model(spark_model=cvModel.bestModel, artifact_path="best_model")

# COMMAND ----------

# MAGIC %md #### Evaluate

# COMMAND ----------

# DBTITLE 1,BinaryClassificationEvaluator
ev.evaluate(predictions)

# COMMAND ----------

predictions.createOrReplaceTempView("predictions")

# COMMAND ----------

# MAGIC %sql select prediction, label from predictions

# COMMAND ----------

# MAGIC %sql 
# MAGIC select sum(case when prediction = label then 1 else 0 end) / (count(1) * 1.0) as accuracy
# MAGIC from predictions

# COMMAND ----------

# MAGIC %md #### Feature Importance

# COMMAND ----------

weights = map(lambda w: '%.10f' % w, cvModel.bestModel.featureImportances)
weightedFeatures = spark.createDataFrame(
    sorted(zip(weights, featureCols), key=lambda x: x[1], reverse=True)
).toDF("weight", "feature")

# COMMAND ----------

display(weightedFeatures.select("feature", "weight").orderBy("weight", ascending=0))

# COMMAND ----------

# MAGIC %python
# MAGIC import time
# MAGIC reportTime = time.strftime('%X %x')
# MAGIC 
# MAGIC html = """
# MAGIC <font face="Verdana" >
# MAGIC <table cellspacing="10">
# MAGIC <tr>
# MAGIC   <tdfont face="Verdana">
# MAGIC   <h1>Wind Turbine Analysis Report</h1>
# MAGIC   <p>Execution completed: <b>{}</b></p>
# MAGIC  </td>
# MAGIC </tr>
# MAGIC </table>
# MAGIC </font>
# MAGIC """.format(reportTime)
# MAGIC 
# MAGIC displayHTML(html)