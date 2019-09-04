# Databricks notebook source
# MAGIC %md
# MAGIC # Predicting Diabetes with Clustering Techniques
# MAGIC 
# MAGIC In this demo notebook, we are trying to predict whether a patient has diabetes based on 
# MAGIC clinical parameters such as insulin, bmi, and more.  To train this classifier, we use a sample dataset
# MAGIC with users with and without diabetes and use KMeans clustering with Spark MLlib.  
# MAGIC 
# MAGIC Here is a diagram that represents the workflow.
# MAGIC 
# MAGIC 
# MAGIC ![Pipeline](https://s3.us-east-2.amazonaws.com/databricks-roy/KM.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 1: Download the dataset to DBFS.

# COMMAND ----------

dbutils.fs.rm("dbfs:/DemoData/diabetes",True)
dbutils.fs.mkdirs("dbfs:/DemoData/diabetes")

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget "https://raw.githubusercontent.com/AvisekSukul/Regression_Diabetes/master/Custom%20Diabetes%20Dataset.csv"

# COMMAND ----------

# MAGIC %sh mv "Custom Diabetes Dataset.csv" /tmp/custom_diabetes_dataset.csv

# COMMAND ----------

# MAGIC %fs cp file:/tmp/custom_diabetes_dataset.csv dbfs:/DemoData/diabetes/custom_diabetes_dataset.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register the data as a Dataframe for SparkSQL.

# COMMAND ----------

df = spark.read.csv('/DemoData/diabetes/custom_diabetes_dataset.csv', header=True, sep=',', inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 3: Do some ad-hoc queries to understand the dataset.

# COMMAND ----------

df.createOrReplaceTempView("diabetes_data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Age vs. Diabetes
# MAGIC 
# MAGIC By graphing age and diabetes, we see that for younger patients, the portion of patients without diabetes is more than with diabetes.

# COMMAND ----------

# MAGIC %sql select age, diabetes, count(*) from diabetes_data group by age, diabetes order by age asc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### BMI vs. Diabetes
# MAGIC When we do a histogram plot, we see that patients with diabetes has higher bmi than those without.

# COMMAND ----------

# MAGIC %sql select bmi, diabetes from diabetes_data where bmi > 0 order by bmi asc

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Step 4: Train the model.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Featurize the data set by assembling a `features` vector.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

train = VectorAssembler(inputCols = ["pregnancies", "plasma glucose", "blood pressure", "triceps skin thickness", "insulin", "bmi", "diabetes pedigree", "age", "diabetes"], outputCol = "features").transform(df)

# COMMAND ----------

display(train)

# COMMAND ----------

# MAGIC %md
# MAGIC Now, run train a bisecting k-means model.

# COMMAND ----------

from pyspark.ml.clustering import KMeans

kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(train)

# COMMAND ----------

model

# COMMAND ----------

wssse = model.computeCost(train)
print("Within Set Sum of Squared Errors = " + str(wssse))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Look at the results of the trained model.

# COMMAND ----------

centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# COMMAND ----------

transformed = model.transform(train)

# COMMAND ----------

display(transformed)

# COMMAND ----------

transformed.createOrReplaceTempView("diabetes_data_predictions")

# COMMAND ----------

# MAGIC %sql select diabetes, prediction, count(*) from diabetes_data_predictions group by diabetes, prediction order by diabetes

# COMMAND ----------

display(transformed.sample(False, fraction = 0.5))

# COMMAND ----------

display(
  transformed.groupBy("prediction").count()
)

# COMMAND ----------

display(
  transformed.groupBy("prediction").avg("insulin")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Insights:
# MAGIC 
# MAGIC ### 1) Average `insulin` level for cluster with `prediction` = `0` is `32.21`. 
# MAGIC ### 2) Average `insulin` level for cluster with `prediction` = `1` is `253.71`.
# MAGIC 
# MAGIC # TL;DR 
# MAGIC ## People with higher `insulin` level can be clubbed to people in cluster `#2` above. This increases the efficacy of predicting a diabetic patient.