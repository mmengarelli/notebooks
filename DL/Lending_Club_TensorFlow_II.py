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

import tensorflow as tf

print(tf.__version__, "GPUs:", tf.config.list_physical_devices('GPU'))
print(tf.keras.__version__)
print(mlflow.__version__)

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
# MAGIC * Tensorflow debugging ✅
# MAGIC * Parameter search ✅ 

# COMMAND ----------

import datetime
import mlflow.tensorflow

mlflow.tensorflow.autolog()

experiment_log_dir = get_user_home() + "/lctf/tensorboard/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=experiment_log_dir)
print("Tensorboard experiment_log_dir:", experiment_log_dir)

# COMMAND ----------

# DBTITLE 0,Build model
from tensorflow.keras import layers, regularizers, Sequential, metrics
from tensorflow.keras.models import Sequential 
from tensorflow.keras.layers import Dense
from tensorflow.keras.optimizers import Adam

def create_model(optimizer='rmsprop', init='glorot_uniform'):
  model = Sequential()
  model.add(Dense(5, input_dim=X.shape[1], kernel_initializer=init, 
                  kernel_regularizer=regularizers.l2(1e-2), activation='relu'))
  #model.add(Dropout(0.2))  
  model.add(Dense(1, activation='sigmoid'))
  
  model.compile(optimizer=optimizer,
                loss='binary_crossentropy',
                metrics=['accuracy',
                        tf.metrics.AUC(name='auc'),
                        tf.metrics.Precision(name='precision'),
                        tf.metrics.Recall(name='recall')])
  return model

# COMMAND ----------

# DBTITLE 1,Setup Tensorboard
experiment_log_dir = get_user_home() + "/lctf/tensorboard/" + datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
print("experiment_log_dir:", experiment_log_dir)
tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=experiment_log_dir)

fit_params = dict(callbacks=[tensorboard_callback])

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

from tensorflow.keras.wrappers.scikit_learn import KerasClassifier
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV

model = KerasClassifier(build_fn=create_model, verbose=2)

optimizers = ['rmsprop']#, 'adam']
inits= ['glorot_uniform']#, 'normal']
epochs = [5]#, 10]

param_grid = dict(optimizer=optimizers, epochs=epochs, init=inits)
grid = GridSearchCV(estimator=model, param_grid=param_grid, cv=3)
grid_result = grid.fit(X, y, **fit_params)

# # summarize results
# print("Best: %f using %s" % (grid_result.best_score_, grid_result.best_params_))
# #mlflow.log_metric("Best Score", grid_result.best_score_)
# #mlflow.log_metric("Best Params", grid_result.best_params_)

# means = grid_result.cv_results_['mean_test_score']
# stds = grid_result.cv_results_['std_test_score']
# params = grid_result.cv_results_['params']

# for mean, stdev, param in zip(means, stds, params):
#   print("%f (%f) with: %r" % (mean, stdev, param))

# COMMAND ----------

# MAGIC %load_ext tensorboard
# MAGIC experiment_log_dir=experiment_log_dir
# MAGIC %tensorboard --logdir $experiment_log_dir

# COMMAND ----------

print("Accuracy: %.2f%% (%.2f%%)" % (results.mean()*100, results.std()*100))


# COMMAND ----------

# MAGIC %%time
# MAGIC model = create_model()
# MAGIC model.fit(X, y, epochs=10, callbacks=[tensorboard_callback])

# COMMAND ----------

model.summary()

# COMMAND ----------

loss, accuracy, auc, precision, recall = model.evaluate(X_test, y_test, verbose=2, callbacks=[tensorboard_callback])

# COMMAND ----------

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
mlflow.end_run()

# COMMAND ----------

