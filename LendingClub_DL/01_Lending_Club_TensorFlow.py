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

# MAGIC %md %pip install mlflow --upgrade

# COMMAND ----------

# MAGIC %run /Users/michael.mengarelli@databricks.com/common_utils_py

# COMMAND ----------

import os, datetime

from sklearn.preprocessing import LabelEncoder, StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split

import tensorflow as tf
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from tensorflow.keras import regularizers, metrics
from tensorflow.python.keras.metrics import AUC, Precision, Recall
import pandas as pd

import mlflow.tensorflow

import warnings
warnings.filterwarnings("ignore")

print("TF Version:", tf.__version__, "GPUs:", tf.config.list_physical_devices('GPU'))
print("Keras Version:", tf.keras.__version__)
print("MLflow Version:", mlflow.__version__)

# COMMAND ----------

def load_data():
  df = table("mikem.loanstats_all").fillna(0).toPandas()
  X = df.loc[:, df.columns != 'bad_loan']
  y = df['bad_loan']
  return X, y

# COMMAND ----------

def prepare_inputs(X_train, X_test):
  ohe = OneHotEncoder(handle_unknown='ignore')
  ohe.fit(X_train)
  X_train_enc = ohe.transform(X_train)
  X_test_enc = ohe.transform(X_test)
  
  return X_train_enc, X_test_enc

# COMMAND ----------

def prepare_targets(y_train, y_test): 
  le = LabelEncoder()
  le.fit(y_train)
  y_train_enc = le.transform(y_train)
  y_test_enc = le.transform(y_test)
  
  return y_train_enc, y_test_enc

# COMMAND ----------

def create_model(input_dim, l2 = 1e-3, lr = 1e-5):
  model = Sequential([
    Dense(17, input_dim=input_dim, kernel_regularizer=regularizers.l2(l2), activation='relu'),
    Dense(1, kernel_regularizer=regularizers.l2(l2), activation='sigmoid')
  ])

  metrics = ['accuracy',
             AUC(),
             Precision(),
             Recall()]

  model.compile(optimizer= Adam(learning_rate=lr),
                loss='binary_crossentropy',
                metrics=metrics)
 
  return model

# COMMAND ----------

X, y = load_data()
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1)

X_train_enc, X_test_enc = prepare_inputs(X_train, X_test)
y_train_enc, y_test_enc = prepare_targets(y_train, y_test)

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC * mlflow logging ✅
# MAGIC * Tensorflow debugging ✅

# COMMAND ----------

mlflow.tensorflow.autolog()

experiment_log_dir = get_user_home() + "/lctf/tensorboard/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
print("experiment_log_dir:", experiment_log_dir)

tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=experiment_log_dir)

# COMMAND ----------

# MAGIC %%time
# MAGIC model = create_model(input_dim=X_train_enc.shape[1], l2=1e-3, lr=1e-5)
# MAGIC model.fit(X_train_enc, y_train_enc, epochs=20, verbose=2, callbacks=[tensorboard_callback])

# COMMAND ----------

loss, accuracy, auc, precision, recall = model.evaluate(X_train_enc, y_train_enc, verbose=2, callbacks=[tensorboard_callback])
print('Accuracy: %.2f' % (accuracy*100))

# COMMAND ----------

preds = model.predict(X_test_enc)

# COMMAND ----------

# MAGIC %md #### Evaluate

# COMMAND ----------

loss, accuracy, auc, precision, recall = model.evaluate(X_test_enc, y_test_enc, verbose=2, callbacks=[tensorboard_callback])

# COMMAND ----------

# DBTITLE 1,Tensorboard
# MAGIC %load_ext tensorboard
# MAGIC experiment_log_dir=experiment_log_dir
# MAGIC %tensorboard --logdir $experiment_log_dir

# COMMAND ----------

mlflow.end_run()
dbutils.tensorboard.stop()