# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC #Evaluating Risk for Loan Approvals
# MAGIC 
# MAGIC ## Business Value
# MAGIC 
# MAGIC Being able to accurately assess the risk of a loan application can save a lender the cost of holding too many risky assets. Rather than a credit score or credit history which tracks how reliable borrowers are, we will generate a score of how profitable a loan will be compared to other loans in the past. The combination of credit scores, credit history, and profitability score will help increase the bottom line for financial institution.
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC <img src="https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png" style="width: 350px;"/>
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data

# COMMAND ----------

# MAGIC %run /Users/michael.mengarelli@databricks.com/common_utils_py

# COMMAND ----------

import tensorflow as tf
from tensorflow.keras import layers, regularizers, Sequential, metrics

import pandas as pd

print(tf.__version__, "GPUs:", tf.config.list_physical_devices('GPU'))

# COMMAND ----------

# MAGIC %md #### Load and prepare training data

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder, OneHotEncoder

def encode_X(X):
  ohe = OneHotEncoder()
  ohe.fit(X)
  X = ohe.transform(X)
  return X

def encode_y(y):
  le = LabelEncoder()
  y = le.fit_transform(y.values.ravel())
  return y

# COMMAND ----------

from sklearn.model_selection import train_test_split

# load data 
df = table("mikem.loanstats_all").fillna(0)

# select features
X = df.drop("bad_loan").toPandas()
X = encode_X(X)  

# select labels
y =  df.select("bad_loan").toPandas()
y = encode_y(y)

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC * mlflow logging ✅

# COMMAND ----------

from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam

def create_model():
  model = Sequential()
  model.add(Dense(5, input_dim=X.shape[1], kernel_regularizer=regularizers.l2(1e-2), activation='relu'))
  model.add(Dense(1, activation='sigmoid'))
  
  model.compile(optimizer=Adam(learning_rate=1e-5),
                loss='binary_crossentropy',
                metrics=['accuracy'])
  return model

# COMMAND ----------

from tensorflow.keras.wrappers.scikit_learn import KerasClassifier

from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import cross_val_score

import mlflow
mlflow.tensorflow.autolog()

model = KerasClassifier(build_fn=create_model, epochs=10, verbose=0)

kfold = StratifiedKFold(n_splits=5, shuffle=True) # define 5-fold cross validation
results = cross_val_score(model, X, y, cv=kfold)
mlflow.log_metric("Mean score: ", results.mean())

# COMMAND ----------

import numpy as np

model = create_model()

kfold = StratifiedKFold(n_splits=5, shuffle=True) # 5-fold cross validation
cvscores = []
i = 0
for train, test in kfold.split(X, y):
  i += 1
  model.fit(X[train], y[train], epochs=10, verbose=0)
  
  scores = model.evaluate(X[test], y[test], verbose=0)
  mlflow.log_metric(model.metrics_names[1], scores[1]*100)
  
  cvscores.append(scores[1] * 100)
  mlflow.log_metric("σ cv_score_{}".format(i), np.std(cvscores))
  mlflow.log_metric("μ cv_score_{}".format(i), np.mean(cvscores))

# COMMAND ----------

mlflow.end_run()