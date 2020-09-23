# Databricks notebook source
import os
import warnings
import sys

import numpy as np
import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import PandasUDFType

import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn import preprocessing
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier

from sklearn.model_selection import RandomizedSearchCV
from scipy.stats import randint
from numpy.random import random

from time import time

# COMMAND ----------

from pyspark.sql.types import * 

iris_schema = StructType([
  StructField("id", IntegerType()),
  StructField("SepalLength", DoubleType()),
  StructField("SepalWidth", DoubleType()),
  StructField("PetalLength", DoubleType()),
  StructField("PetalWidth", DoubleType()),
  StructField("Species", StringType())
])

iris_pd = spark.read.option("header","true").schema(iris_schema) \
  .csv("/databricks-datasets/Rdatasets/data-001/csv/datasets/iris.csv").toPandas()

display(iris_pd)

# COMMAND ----------

# Define a function to append the same dataframe n times with random ids
def concat_dataframe(df,n):
  concat_df = df.append([df]*n,ignore_index=True)
  print(concat_df.shape)
  # Updating the Id column with a randint(1,n) to simulate n datasets to train later
  concat_df["id"] = np.random.randint(1, n+1, concat_df.shape[0]) # numpy.randint(low (inclusive) to high (exclusive))
  return concat_df

# COMMAND ----------

num_concat = 100 # number of series to create
iris = concat_dataframe(iris_pd,num_concat)
print(iris.head())

# COMMAND ----------

# Checking the number of distinct IDs=series
print(iris["id"].unique().shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Train single SK-Learn Model

# COMMAND ----------

def train(df):
  ref_id = df['id'].iloc[0]     # get the value of this group id
  num_instances = df.shape[0]   # get the number of training instances for this group
   
  df["Species"] = preprocessing.LabelEncoder().fit_transform(df["Species"])

  species = df.Species # the Species is the value we want to predict
  features = df.drop(["Species","SepalLength","SepalWidth",],axis=1) # selecting only the "petal" features

  X_train, X_test, y_train, y_test = train_test_split(features, species, test_size=0.3)

  n_estimators = 100
  max_depth = 2
  random_state = 42

  random_forest = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth,random_state=random_state)

  param_dist = {
    "max_depth": randint(3,6),
    "n_estimators": randint(5,20),
    "min_samples_split": randint(2, 5)
  }

  iters = 20
  random_search = RandomizedSearchCV(random_forest, param_distributions=param_dist,
                                 n_iter=iters, cv=5, iid=False)

  random_search.fit(X_train, y_train)

  print(random_search.best_estimator_.get_params())
  print(random_search.best_estimator_.get_params()["max_depth"])

  predictions = random_search.predict(X_test)

  accuracy = metrics.accuracy_score(predictions,y_test)
  print(metrics.accuracy_score(predictions,y_test))
  
  df_to_return = pd.DataFrame([[ref_id, num_instances, accuracy]], columns = ['id', 'num_instances_trained_with', 'accuracy'])
  return df_to_return

# COMMAND ----------

# DBTITLE 1,Train on single series
df = iris[iris['id']==42] # select a single series/id to train on
predictions = train(df) 

# COMMAND ----------

display(predictions)

# COMMAND ----------

# MAGIC %md ## 2. Train 1000 models in parallel

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
  StructField('id', IntegerType()),
  StructField('num_instances_trained_with', IntegerType()),
  StructField('accuracy', FloatType())  
])

# COMMAND ----------

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def train_udf(df):
  return train(df)

# COMMAND ----------

# DBTITLE 1,Train on all series, grouped by id
iris = concat_dataframe(iris_pd, 10)
df = spark.createDataFrame(pd.DataFrame(iris))

# COMMAND ----------

predictions = df.groupBy('id').apply(train_udf)

# COMMAND ----------

display(predictions)