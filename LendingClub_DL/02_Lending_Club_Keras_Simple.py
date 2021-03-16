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

from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline

import tensorflow as tf
from tensorflow.keras.models import Sequential, Model
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam
from tensorflow.keras import regularizers, metrics
from tensorflow.keras.wrappers.scikit_learn import KerasClassifier

import mlflow

import warnings
warnings.filterwarnings("ignore")

print("TF Version:", tf.__version__, "GPUs:", tf.config.list_physical_devices('GPU'))
print("Keras Version:", tf.keras.__version__)
print("MLflow Version:", mlflow.__version__)

# COMMAND ----------

# MAGIC %run /Users/michael.mengarelli@databricks.com/common_utils_py

# COMMAND ----------

def load_data():
  df = table("mikem.loanstats_all").fillna(0).toPandas()
  X = df.loc[:, df.columns != 'bad_loan']
  y = df['bad_loan']
  return X, y

# COMMAND ----------

def encode_labels(y): 
  enc = LabelEncoder()
  enc.fit(y)
  return enc.transform(y)

# COMMAND ----------

def create_model(input_dim=24064, l2=1e-5, lr=1e-5):
  model = Sequential()
  model.add(Dense(17, input_dim=input_dim, kernel_regularizer=regularizers.l2(1e-2), activation='relu'))
  model.add(Dense(8, kernel_regularizer=regularizers.l2(1e-2), activation='relu'))
  model.add(Dense(1, activation='sigmoid'))
  
  model.compile(optimizer=Adam(learning_rate=1e-5),
                loss='binary_crossentropy',
                metrics=['accuracy'])
  return model

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC * mlflow logging ✅
# MAGIC * K-fold cross validation ✅

# COMMAND ----------

X, y = load_data()
y_enc = encode_labels(y)

ohe = OneHotEncoder(handle_unknown='ignore')
ohe.fit(X)

#mlflow.tensorflow.autolog()

clf = KerasClassifier(build_fn=lambda: create_model(input_dim=24124, l2=1e-3, lr=1e-5), epochs=10, verbose=0)

estimators = []
estimators.append(('ohe', ohe))
estimators.append(('mlp', clf))
pipeline = Pipeline(estimators)

skf = StratifiedKFold(n_splits=5, shuffle=True)

results = cross_val_score(pipeline, X, y_enc, cv=skf)
results
#print("Standardized: %.2f%% (%.2f%%)" % (results.mean()*100, results.std()*100))

# COMMAND ----------

X, y = load_data()
y_enc = encode_labels(y)

ohe = OneHotEncoder(handle_unknown='ignore')
ohe.fit(X)

clf = KerasClassifier(lambda: create_model(input_dim=24064, l2=1e-3, lr=1e-5), epochs=10, verbose=0)

estimators = []
estimators.append(('ohe', ohe))
estimators.append(('mlp', clf))
pipeline = Pipeline(estimators)

skf = StratifiedKFold(n_splits=5, shuffle=True)

pipeline = pipeline.fit(X, y_enc)

# COMMAND ----------

clf.predict(X)

# COMMAND ----------

pipeline.transform()

# COMMAND ----------

skf = StratifiedKFold(n_splits=5, shuffle=True)

results = cross_val_score(pipeline, X, y_enc, cv=skf)
print("Standardized: %.2f%% (%.2f%%)" % (results.mean()*100, results.std()*100))

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

