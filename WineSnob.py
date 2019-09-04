# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC # Predicting Wine Quality
# MAGIC We will train a `Decision Tree Regression` model using the following [data set](http://archive.ics.uci.edu/ml/datasets/Wine+Quality) to predcit wine quality. This includes red and white vinho verde wine samples from the north of Portugal. 
# MAGIC 
# MAGIC ###### Here's the list of all the features:
# MAGIC 
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/mikem-docs/img/winesnob.jpeg" style="float:right; height: 250px; margin: 10px; border: 1px solid #ddd; border-radius: 15px 15px 15px 15px; padding: 10px"/>
# MAGIC 
# MAGIC * quality (target)
# MAGIC * fixed acidity
# MAGIC * volatile acidity
# MAGIC * citric acid
# MAGIC * residual sugar
# MAGIC * chlorides
# MAGIC * free sulfur dioxide
# MAGIC * total sulfur dioxide
# MAGIC * density
# MAGIC * pH
# MAGIC * sulphates
# MAGIC * alcohol

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import *
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# MAGIC %md ##Explore

# COMMAND ----------

df = spark.read \
.option("header", "true") \
.option("inferSchema", "true") \
.option("delimiter", ";") \
.csv("/mikem/wine-quality/winequality-red.csv") \

display(df)

# COMMAND ----------

cols = df.columns
for (a,b) in enumerate(zip(range(0, len(cols)), cols)):
  print (b)

# COMMAND ----------

# MAGIC %md ##ML Workflow

# COMMAND ----------

# DBTITLE 1,Feature engineering
features = cols.remove('quality')
va = VectorAssembler(inputCols=cols, outputCol='indexedFeatures')

# COMMAND ----------

# DBTITLE 1,Train
(train, test) = df.randomSplit([0.7, 0.3])
dt = DecisionTreeRegressor(maxDepth=10, featuresCol='indexedFeatures', labelCol='quality')

pipeline = Pipeline(stages=[va, dt])
model = pipeline.fit(train)
# model.save("/mnt/mikem/wine-snob/model/")

# COMMAND ----------

# DBTITLE 1,Predictions
predictions = model.transform(test)
predictions.registerTempTable('predictions')
display(spark.sql('select quality, prediction from predictions order by quality desc'))

# COMMAND ----------

# DBTITLE 1,Evaluate
# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(labelCol="quality", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# COMMAND ----------

treeModel = model.stages[1]
print(treeModel)