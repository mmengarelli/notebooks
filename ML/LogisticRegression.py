# Databricks notebook source
# MAGIC %md # Logistic Regression
# MAGIC 
# MAGIC #### Hypothesis
# MAGIC <img src="http://4.bp.blogspot.com/-c-C6IGeS_eo/TraZSGODaCI/AAAAAAAAAog/lspXBBwj7FE/s1600/Screen+shot+2011-11-06+at+11.26.53+AM.png"/>

# COMMAND ----------

import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md #### Sigmoid Function
# MAGIC 
# MAGIC <img src="http://2.bp.blogspot.com/-PqGvtE_NEmE/TraZYTHEfLI/AAAAAAAAAoo/swDZg20vd4Q/s1600/Screen+shot+2011-11-06+at+11.27.03+AM.png"/>

# COMMAND ----------

# DBTITLE 0,Sigmoid
def sigmoid(z): 
  return 1/(1 + np.exp(-z))

# COMMAND ----------

# test sigmoid = .5 
sigmoid(0)

# COMMAND ----------

x = np.linspace(-100, 100, 200) 
z = 1/(1 + np.exp(-x)) 
  
plt.plot(x, z) 
plt.xlabel("x") 
plt.ylabel("Sigmoid(X)") 
  
plt.show() 

# COMMAND ----------

# MAGIC %md #### Cost Function (without regularization)
# MAGIC To compute the cost function J(Θ) and gradient (partial derivative of J(Θ) with respect to each Θ)
# MAGIC 
# MAGIC Cost<br/>
# MAGIC <img src="http://4.bp.blogspot.com/-0vWgkEmE-u4/TraaI_rd-bI/AAAAAAAAAow/Ya5rp0rQS48/s400/Screen+shot+2011-11-06+at+11.30.37+AM.png">
# MAGIC 
# MAGIC Gradient<br/>
# MAGIC <img src="http://2.bp.blogspot.com/-jpwtW1KQIoE/TraaRvy_8MI/AAAAAAAAAo4/9qnO3SyiqaA/s320/Screen+shot+2011-11-06+at+11.30.41+AM.png"/>

# COMMAND ----------

# compute cost and gradient vector
def cost_function(theta, X, y):
  m = len(y)
  h = sigmoid(np.dot(X,theta))
  error = (-y * np.log(h)) - ((1 - y) * np.log(1 - h))
  J = 1/m * sum(error)
  gradient = 1/m * np.dot(X.T,(h - y))

  return J[0], gradient

# COMMAND ----------

def compute_cost(X, y, theta):
  m = X.shape[0]
  z = np.dot(X,theta)            
  h = sigmoid(z);    

  J=(float(-1)/m)*((y.T.dot(np.log(h))) + ((1 - y.T).dot(np.log(1 - h))))           
  return J

# COMMAND ----------

# Dataset: testscore_1, testscore_2, admission_status
df = pd.read_csv("/dbfs/mnt/mcm/data/log_reg.csv", header=None)
df.head()

X = df[[0,1]]
y = df[[2]]

# COMMAND ----------

# test cost function with initial theta
m, n = np.shape(X) 

X = np.append(np.ones((m, 1)), X, axis = 1)
y = y.values.reshape(m, 1)

initial_theta = np.zeros((n + 1, 1)) # 4 x 1
cost, gradient = cost_function(initial_theta, X, y)

print("Cost of initial theta: ", cost) # ~0.693
print("Gradient at initial theta: ", gradient) #  [-0.1000, -12.0092, -11.2628]

# COMMAND ----------

# MAGIC %md #### Optimize for theta

# COMMAND ----------

# MAGIC %md #### Full Solution
# MAGIC Motivated by [this](https://gist.github.com/dormantroot/4223554)

# COMMAND ----------

from scipy.optimize import fmin_bfgs
import scipy.optimize as opt

df = pd.read_csv("/dbfs/mnt/mcm/data/log_reg.csv", header=None)

X = df[[0,1]]
y = df[[2]]

m, n = X.shape[0], X.shape[1]
X = np.append(np.ones((m,1)), X, axis=1)
y = y.values.reshape(m,1)

initial_theta = np.zeros((n + 1, 1))
cost, grad = cost_function(initial_theta,X,y)

print("Cost of initial theta is",cost)
print("Gradient at initial theta (zeros):",grad)

def compute_gradient(X, y, theta):
  m = X.shape[0]
  z = np.dot(X,theta)            
  h = sigmoid(z);

  grad = (float(1)/m)*((h-y).T.dot(X))          
  return grad

def f(theta):
  return np.ndarray.flatten(compute_cost(X, y, initial_theta))
            
def fprime(theta):
  return np.ndarray.flatten(compute_gradient(X, y, initial_theta))

opt = fmin_bfgs(f, initial_theta, fprime, disp=True, maxiter=400, full_output = True, retall=True)
opt