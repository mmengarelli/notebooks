# Databricks notebook source
# MAGIC %md # Linear Regression
# MAGIC * Vectorized Cost Function
# MAGIC * Gradient Descent

# COMMAND ----------

import numpy as np
import pandas as pd

# COMMAND ----------

df2 = pd.read_csv("/dbfs/mnt/mcm/data/lr.csv")

X = df2.loc[:,["X"]]
y = df2.loc[:,["y"]]

# COMMAND ----------

# MAGIC %md #### Cost Function
# MAGIC <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/cost_func.png"/>

# COMMAND ----------

def compute_cost(X, y, theta):
  m = len(y)
  h = np.dot(X, theta) 
  sqd_error = (h-y).T@(h-y)
  J = np.sum(sqd_error/(2*m))
  # J = sqd_error/(2*m)
  return J

# COMMAND ----------

# MAGIC %md #### Gradient Descent
# MAGIC <img src="https://mikem-docs.s3-us-west-2.amazonaws.com/img/grad_desc.png"/>

# COMMAND ----------

def gradient_descent(X, y, theta, alpha, iterations):
  for i in range(0, iterations):
    m = len(X)
    h = np.dot(X, theta)
    loss = h - y
    cost = np.sum(loss ** 2) / (2 * m)
    print("Iteration %d | Cost: %f" % (i, cost))
    gradient = np.dot(X.T, loss) / m
    theta = theta - alpha * gradient
  return theta

# COMMAND ----------

# DBTITLE 1,Learn
theta = pd.DataFrame({'theta' : pd.Series([-1, 2])}) # init theta
X = np.hstack([np.ones([X.size, 1]), X])

# Compute Cost J 
J = compute_cost(X, y, theta)
print('Cost: ', J, "for theta ", theta)

# Run Gradient Descent
iterations = 1500
alpha = 0.01

theta = gradient_descent(X, y, theta, alpha, iterations)

# COMMAND ----------

# DBTITLE 1,Plot the fit
import matplotlib.pyplot as plt

x = X[:,1]
y = np.dot(X, theta)

plt.style.use('seaborn')
plt.plot(x, y)
plt.title('Model Fit', fontsize = 18, y = 1.03)
plt.legend()


# COMMAND ----------

# DBTITLE 1,Predict
predict1 = np.dot([1, 3.5], theta)
print(predict1*10000)