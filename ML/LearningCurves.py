# Databricks notebook source
# MAGIC %md # Detecting Bias and Variance
# MAGIC 
# MAGIC Here we will plot the learning curves for a learning algorithm to diagnose whether our algorithm suffers from bias or variance. The data set we will be using is power plant data found [here](https://www.dataquest.io/blog/learning-curves-machine-learning/). We will use linear regression to predict power output.
# MAGIC 
# MAGIC #### Feature Set
# MAGIC * Temperature (T) in the range 1.81°C and 37.11°C
# MAGIC * Ambient Pressure (AP) in the range 992.89-1033.30 milibar
# MAGIC * Relative Humidity (RH) in the range 25.56% to 100.16%
# MAGIC * Exhaust Vacuum (V) in teh range 25.36-81.56 cm Hg
# MAGIC * Net hourly electrical energy output (EP) 420.26-495.76 MW
# MAGIC 
# MAGIC This experiment was largely motivated by the following [blog](https://www.dataquest.io/blog/learning-curves-machine-learning/).

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget https://archive.ics.uci.edu/ml/machine-learning-databases/00294/CCPP.zip
# MAGIC unzip -d /data CCPP.zip

# COMMAND ----------

# Get excel reader
%pip install xlrd

# COMMAND ----------

import pandas as pd

electricity = pd.read_excel('/data/CCPP/Folds5x2_pp.xlsx')

print(electricity.info())
electricity.head(3)

# COMMAND ----------

electricity.size

# COMMAND ----------

train_sizes = [1, 100, 500, 2000, 5000, 7654]

# COMMAND ----------

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import learning_curve

features = ['AT', 'V', 'AP', 'RH']
target = 'PE'

# learning_curve() runs a k-fold cross-validation under the hood
# the value of k is specified by the cv parameter
# This will return scores using negative MSE
train_sizes, train_scores, validation_scores = learning_curve(
  estimator = LinearRegression(),
  X = electricity[features],
  y = electricity[target], train_sizes = train_sizes, cv = 5,
  scoring = 'neg_mean_squared_error')


# COMMAND ----------

# To plot the learning curves, we need only a single error score per training set size
# For this reason we take the mean value of each row and also flip the signs of the error scores
train_scores_mean = -train_scores.mean(axis = 1)
validation_scores_mean = -validation_scores.mean(axis = 1)

print('Mean Training scores:', train_scores_mean)
print('\nMean Validation scores:', validation_scores_mean)

# COMMAND ----------

# MAGIC %md The plot below will show that our learning algorithm suffers from high bias. 
# MAGIC Performance (MSE) seems to level off at 2000 examples, so adding more data likely will not help. 

# COMMAND ----------

import matplotlib.pyplot as plt

plt.style.use('seaborn')
plt.plot(train_sizes, train_scores_mean, label = 'Training error')
plt.plot(train_sizes, validation_scores_mean, label = 'Validation error')
plt.ylabel('MSE', fontsize = 14)
plt.xlabel('Training set size (m)', fontsize = 14)
plt.title('Learning curves for a linear regression model', fontsize = 18, y = 1.03)
plt.legend()
plt.ylim(0,40)