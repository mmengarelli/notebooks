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

from sklearn.preprocessing import LabelEncoder, StandardScaler, OrdinalEncoder, OneHotEncoder

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

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=27)

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC * mlflow logging ✅
# MAGIC * Tensorflow debugging ✅

# COMMAND ----------

import os, datetime
import mlflow.tensorflow

mlflow.tensorflow.autolog()

experiment_log_dir = get_user_home() + "/lctf/tensorboard/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
print("experiment_log_dir:", experiment_log_dir)

tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=experiment_log_dir)

# COMMAND ----------

# DBTITLE 1,Build model
l2 = 2e-3 # reg term
model = Sequential([
  layers.Dense(5, input_dim=X.shape[1], kernel_regularizer=regularizers.l2(l2), activation=tf.nn.relu),
  layers.Dense(1, kernel_regularizer=regularizers.l2(l2), activation=tf.nn.sigmoid)
])

metrics = [
  'accuracy',
  tf.metrics.AUC(name='auc'),
  tf.metrics.Precision(name='precision'),
  tf.metrics.Recall(name='recall')
]

optimizer = tf.keras.optimizers.Adam(
    learning_rate=1e-5
)

model.compile(
  optimizer=optimizer,
  loss='binary_crossentropy',
  metrics=metrics
)

# COMMAND ----------

# MAGIC %%time
# MAGIC model.fit(X, y, validation_data=(X_test, y_test), epochs=25, callbacks=[tensorboard_callback])

# COMMAND ----------

model.summary()

# COMMAND ----------

loss, accuracy, auc, precision, recall = model.evaluate(X_test, y_test, verbose=2, callbacks=[tensorboard_callback])

# COMMAND ----------

import numpy as np

predicted_classes = model.predict_classes(X_test)

correct_indices = np.nonzero(predicted_classes == y_test)[0]
incorrect_indices = np.nonzero(predicted_classes != y_test)[0]

print("Correct: ", correct_indices.shape[0])
print("Incorrect: ", incorrect_indices.shape[0])

# COMMAND ----------

# DBTITLE 1,Tensorboard
# MAGIC %load_ext tensorboard
# MAGIC experiment_log_dir=experiment_log_dir
# MAGIC %tensorboard --logdir $experiment_log_dir

# COMMAND ----------

dbutils.tensorboard.stop()