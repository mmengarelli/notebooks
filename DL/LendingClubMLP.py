# Databricks notebook source
# MAGIC %md Loan classification using **Multi-layer Perceptron classifier**

# COMMAND ----------

# MAGIC %md #### Load and prepare data 

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler #, OneHotEncoder
from sklearn.model_selection import train_test_split

import pandas as pd

df = table("mikem.loanstats_all").fillna(0)

# Choose independent vars
X = df.drop("bad_loan").toPandas()
X = pd.get_dummies(X, prefix_sep='_', drop_first=True)

# Chose target and label encode
y =  df.select("bad_loan").toPandas()

le = LabelEncoder()
y = le.fit_transform(y.values.ravel())

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=27)

# COMMAND ----------

from sklearn.metrics import mean_squared_error, r2_score, f1_score

def eval_metrics(y_true, y_pred):
  mse = mean_squared_error(y_true, y_pred)
  r2 = r2_score(y_true, y_pred)
  f1 = f1_score(y_true, y_pred)

  return mse, r2, f1

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC Multi-layer Perceptron is sensitive to feature scaling, so it is highly recommended to scale your data. Here we will use **Standard Scaler** to standardize on mean 0 and variance 1. 

# COMMAND ----------

from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline

import mlflow

with mlflow.start_run():
  num_neurons = len(X_train.columns)
  alpha = 1e-4
  
  clf = MLPClassifier(solver='lbfgs', 
                      alpha=alpha, # L2 penalty/regularization parameter
                      activation='logistic',
                      hidden_layer_sizes=(num_neurons, ), 
                      max_iter=500, 
                      random_state=27)

  mlflow.log_param('alpha', alpha)
  mlflow.log_param('activation', 'logistic')
  mlflow.log_param('hidden_layer_sizes', num_neurons)
  
  pipeline = Pipeline([('scaler', StandardScaler()), ('clf', clf)])
  model = pipeline.fit(X_train, y_train)

  mlflow.sklearn.log_model(model, 'model')

# COMMAND ----------

preds = model.predict(X_test)
score = model.score(X_test, y_test)

(mse, r2, f1) = eval_metrics(y_test, preds)

mlflow.log_metric('test score', score)
mlflow.log_metric('test mse', mse)
mlflow.log_metric('test r2', r2)
mlflow.log_metric('test f1', f1)