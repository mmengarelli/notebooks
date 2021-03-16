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

# MAGIC %run /Users/michael.mengarelli@databricks.com/common_utils_py

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

def encode_inputs(X):
  ohe = OneHotEncoder()
  ohe.fit(X)
  X = ohe.transform(X)
  return X

# COMMAND ----------

def encode_labels(y): 
  enc = LabelEncoder()
  enc.fit(y)
  return enc.transform(y)

# COMMAND ----------

def build_model(optimizer='rmsprop', init='glorot_uniform'):
  model = Sequential()
  model.add(Dense(17, input_dim=24064, kernel_initializer=init, 
                  kernel_regularizer=regularizers.l2(1e-2), activation='relu'))
  #model.add(Dropout(0.2))  
  model.add(Dense(1, activation='sigmoid'))
  
  model.compile(optimizer=optimizer,
                loss='binary_crossentropy',
                metrics=['accuracy'])
  
  return model

# COMMAND ----------

X, y = load_data()
X_enc = encode_inputs(X)
y_enc = encode_labels(y)

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC * mlflow logging ✅
# MAGIC * Tensorflow debugging ✅
# MAGIC * Parameter search ✅ 

# COMMAND ----------

experiment_log_dir = get_user_home() + "/lctf/tensorboard/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
print("experiment_log_dir:", experiment_log_dir)
tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=experiment_log_dir)

fit_params = dict(callbacks=[tensorboard_callback])

# COMMAND ----------

mlflow.tensorflow.autolog()

model = KerasClassifier(build_fn=build_model, verbose=2)

# Test with small/few parameters
optimizers = ['rmsprop'] # , 'adam']
inits= ['glorot_uniform', 'normal']
epochs = [3, 5]  

param_grid = dict(optimizer=optimizers, epochs=epochs, init=inits)
grid = GridSearchCV(estimator=model, param_grid=param_grid, cv=3)

grid_result = grid.fit(X_enc, y_enc, **fit_params)

# COMMAND ----------

mlflow.end_run()