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

# MAGIC %pip install mlflow --upgrade

# COMMAND ----------

# MAGIC %run ./common_utils

# COMMAND ----------

import datetime

import tensorflow as tf
from tensorflow.keras.wrappers.scikit_learn import KerasClassifier
from tensorflow.keras import layers, regularizers, metrics
from tensorflow.keras.models import Sequential 
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from tensorflow.python.keras.metrics import AUC, Precision, Recall

from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.model_selection import train_test_split

import matplotlib.pyplot as plt

import mlflow 
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

def encode_inputs(X_train, X_test):
  ohe = OneHotEncoder(handle_unknown='ignore')
  ohe.fit(X_train)
  X_train_enc = ohe.transform(X_train)
  X_test_enc = ohe.transform(X_test)
  
  return X_train_enc, X_test_enc

# COMMAND ----------

def encode_labels(y_train, y_test): 
  le = LabelEncoder()
  le.fit(y_train)
  y_train_enc = le.transform(y_train)
  y_test_enc = le.transform(y_test)
  
  return y_train_enc, y_test_enc

# COMMAND ----------

def build_model(input_dim,optimizer='rmsprop', init='glorot_uniform'):
  model = Sequential()
  model.add(Dense(17, 
                  kernel_initializer=init, 
                  input_dim=input_dim,
                  kernel_regularizer=regularizers.l2(1e-2), 
                  activation='relu'))
  #model.add(Dropout(0.2))  
  model.add(Dense(1, activation='sigmoid'))
  
  model.compile(optimizer=optimizer,
                loss='binary_crossentropy',
                metrics=['accuracy'])
  
  return model

# COMMAND ----------

X, y = load_data()
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1)

X_train_enc, X_test_enc = encode_inputs(X_train, X_test)
y_train_enc, y_test_enc = encode_labels(y_train, y_test)

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC * mlflow logging ✅
# MAGIC * Parameter search ✅ 

# COMMAND ----------

mlflow.tensorflow.autolog()

model = KerasClassifier(build_fn=build_model, input_dim=X_train_enc.shape[1], verbose=2)

optimizers = ['rmsprop', 'adam']
inits= ['glorot_uniform', 'normal']
epochs = [5, 10, 15]

param_grid = dict(optimizer=optimizers, epochs=epochs, init=inits)
grid = GridSearchCV(estimator=model, param_grid=param_grid, cv=3)

grid_result = grid.fit(X_train_enc, y_train_enc, use_multiprocessing=True)
best_model = grid_result.best_estimator_.model
history = best_model.history.history

# COMMAND ----------

mlflow.log_artifact(plot_accuracy(history))
mlflow.log_artifact(plot_loss(history))

# COMMAND ----------

import pandas as pd

pd.DataFrame(grid.cv_results_).head()

# COMMAND ----------

print(grid.best_params_)
print(grid.best_score_)

# COMMAND ----------

preds = best_model.evaluate(X_test_enc, y_test_enc)

# COMMAND ----------

mlflow.log("test loss: ", preds[0])
mlflow.log("test accuracy: ", preds[1])

# COMMAND ----------

mlflow.end_run()