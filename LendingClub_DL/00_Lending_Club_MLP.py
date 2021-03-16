# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC #Evaluating Risk for Loan Approvals Using Multilayer Perceptron
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

# MAGIC %md #### Load and prepare data 

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, f1_score

from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline

import mlflow
import mlflow.sklearn

import warnings
warnings.filterwarnings("ignore")

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

def eval_metrics(y_true, y_pred):
  mse = mean_squared_error(y_true, y_pred)
  r2 = r2_score(y_true, y_pred)
  f1 = f1_score(y_true, y_pred)

  return mse, r2, f1

# COMMAND ----------

X, y = load_data()
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1)

X_train_enc, X_test_enc = prepare_inputs(X_train, X_test)
y_train_enc, y_test_enc = prepare_targets(y_train, y_test)

# COMMAND ----------

# MAGIC %md #### Train
# MAGIC Multi-layer Perceptron is sensitive to feature scaling, so it is highly recommended to scale your data. Here we will use **Standard Scaler** to standardize on mean 0 and variance 1. 

# COMMAND ----------

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

pipeline = Pipeline([('scaler', StandardScaler(with_mean=False)), ('clf', clf)])
model = pipeline.fit(X_train_enc, y_train_enc)

mlflow.sklearn.log_model(model, 'model')

# COMMAND ----------

preds = model.predict(X_test_enc)
score = model.score(X_test_enc, y_test_enc)

# COMMAND ----------

# MAGIC %md #### Evaluate

# COMMAND ----------

# Log performance 
(mse, r2, f1) = eval_metrics(y_test_enc, preds)

mlflow.log_metric('test score', score)
mlflow.log_metric('test mse', mse)
mlflow.log_metric('test r2', r2)
mlflow.log_metric('test f1', f1)

# COMMAND ----------

from sklearn.metrics import classification_report,confusion_matrix

print(confusion_matrix(y_test_enc, preds))

# COMMAND ----------

print(classification_report(y_test_enc, preds))

# COMMAND ----------

mlflow.end_run()