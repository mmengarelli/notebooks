# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Experiment Tracking
# MAGIC 
# MAGIC The machine learning life cycle involves training multiple algorithms using different hyperparameters and libraries, all with different performance results and trained models.  This lesson explores tracking those experiments to organize the machine learning life cycle.
# MAGIC 
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC  - Introduce tracking ML experiments in MLflow
# MAGIC  - Log an experiment and explore the results in the UI
# MAGIC  - Record parameters, metrics, and a model
# MAGIC  - Query past runs programatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Classroom-Setup
# MAGIC 
# MAGIC For each lesson to execute correctly, please make sure to run the **`Classroom-Setup`** cell at the start of each lesson (see the next cell).

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Over the course of the machine learning life cycle...<br><br>
# MAGIC 
# MAGIC * Data scientists test many different models
# MAGIC * Using various libraries
# MAGIC * Each with different hyperparameters
# MAGIC 
# MAGIC Tracking these various results poses an organizational challenge, including... <br><br>
# MAGIC 
# MAGIC * Storing experiments
# MAGIC * Results
# MAGIC * Models
# MAGIC * Supplementary artifacts
# MAGIC * Code
# MAGIC * Data snapshots

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC MLflow Tracking is...<br><br>
# MAGIC 
# MAGIC * a logging API specific for machine learning
# MAGIC * agnostic to libraries and environments that do the training
# MAGIC * organized around the concept of **runs**, which are executions of data science code
# MAGIC * runs are aggregated into **experiments** where many runs can be a part of a given experiment
# MAGIC * An MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:<br><br>
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiment
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Experiment Logging and UI
# MAGIC 
# MAGIC MLflow is an open source software project developed by Databricks available to developers regardless of which platform they are using.  Databricks hosts MLflow for you, which reduces deployment configuration and adds security benefits.  It is accessible on all Databricks workspaces in Azure and AWS.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> See <a href="https://mlflow.org/docs/latest/quickstart.html#" target="_blank">the MLflow quickstart guide</a> for details on setting up MLflow locally or on your own server.

# COMMAND ----------

# MAGIC %md
# MAGIC Import a dataset of Airbnb listings and featurize the data.  We'll use this to train a model.

# COMMAND ----------

import pandas as pd

df = pd.read_csv("/dbfs/mnt/training/airbnb/sf-listings/airbnb-cleaned-mlflow.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Perform a train/test split.

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1), df[["price"]].values.ravel(), random_state=42)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC **Navigate to the MLflow UI by clicking on the `Runs` button on the top of the screen.**
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Every Python notebook in a Azure Databricks Workspace has its own experiment. When you use MLflow in a notebook, it records runs in the notebook experiment. A notebook experiment shares the same name and ID as its corresponding notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC Log a basic experiment by doing the following:<br><br>
# MAGIC 
# MAGIC 1. Start an experiment using `mlflow.start_run()` and passing it a name for the run
# MAGIC 2. Train your model
# MAGIC 3. Log the model using `mlflow.sklearn.log_model()`
# MAGIC 4. Log the model error using `mlflow.log_metric()`
# MAGIC 5. Print out the run id using `run.info.run_id`

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

with mlflow.start_run(run_name="Basic RF Experiment") as run:
  # Create model, train it, and create predictions
  rf = RandomForestRegressor(random_state=42)
  rf.fit(X_train, y_train)
  predictions = rf.predict(X_test)

  # Log model
  mlflow.sklearn.log_model(rf, "random-forest-model")

  # Create metrics
  mse = mean_squared_error(y_test, predictions)
  print(f"mse: {mse}")

  # Log metrics
  mlflow.log_metric("mse", mse)

  runID = run.info.run_id
  experimentID = run.info.experiment_id

  print(f"Inside MLflow Run with run_id `{runID}` and experiment_id `{experimentID}`")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Examine the results in the UI.  Look for the following:<br><br>
# MAGIC 
# MAGIC 1. The `Experiment ID`
# MAGIC 2. The artifact location.  This is where the artifacts are stored in DBFS, which is backed by cloud storage.
# MAGIC 3. The time the run was executed.  **Click this timestamp to see more information on the run.**
# MAGIC 4. The code that executed the run.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/mlflow/mlflow_ui_page.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC After clicking on the time of the run, take a look at the following:<br><br>
# MAGIC 
# MAGIC 1. The Run ID will match what we printed above
# MAGIC 2. The model that we saved, included a picked version of the model as well as the Conda environment and the `MLmodel` file, which will be discussed in the next lesson.
# MAGIC 
# MAGIC <div><img src="http://files.training.databricks.com/images/mlflow/mlflow_model_page.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Parameters, Metrics, and Artifacts
# MAGIC 
# MAGIC But wait, there's more!  In the last example, you logged the run name, an evaluation metric, and your model itself as an artifact.  Now let's log parameters, multiple metrics, and other artifacts including the feature importances.
# MAGIC 
# MAGIC First, create a function to perform this.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> To log artifacts, we have to save them somewhere before MLflow can log them.  This code accomplishes that by using a temporary file that it then deletes.

# COMMAND ----------

# MAGIC %pip list | grep mlflow

# COMMAND ----------

def log_rf(experimentID, run_name, params, X_train, X_test, y_train, y_test):
  import os
  import matplotlib.pyplot as plt
  import mlflow.sklearn
  import seaborn as sns
  from sklearn.ensemble import RandomForestRegressor
  from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
  import tempfile

  with mlflow.start_run(experiment_id=experimentID, run_name=run_name) as run:
    # Create model, train it, and create predictions
    rf = RandomForestRegressor(**params)
    rf.fit(X_train, y_train)
    predictions = rf.predict(X_test)

    # Log model
    mlflow.sklearn.log_model(rf, "random-forest-model")

    # Log params
    mlflow.log_params(params)

    # Create metrics
    mse = mean_squared_error(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    # Log metrics
    mlflow.log_metrics({"mse": mse, "mae": mae, "r2": r2})

    # Create feature importance
    importance = pd.DataFrame(list(zip(df.columns, rf.feature_importances_)),
                                columns=["Feature", "Importance"]
                              ).sort_values("Importance", ascending=False)

    # Log importances using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="feature-importance-", suffix=".csv")
    temp_name = temp.name
    try:
      importance.to_csv(temp_name, index=False)
      mlflow.log_artifact(temp_name, "feature-importance.csv")
    finally:
      temp.close() # Delete the temp file

    # Create plot
    fig, ax = plt.subplots()

    importance.plot.bar(ax=ax)
    plt.xlabel("Predicted values for Price ($)")
    plt.ylabel("Residual")
    plt.title("Residual Plot")

    # Log residuals using a temporary file
    temp = tempfile.NamedTemporaryFile(prefix="residuals-", suffix=".png")
    temp_name = temp.name
    try:
      fig.savefig(temp_name)
      mlflow.log_artifact(temp_name, "residuals.png")
    finally:
      temp.close() # Delete the temp file

    display(fig)
    return run.info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC Run with new parameters.

# COMMAND ----------

params = {
  "n_estimators": 100,
  "max_depth": 5,
  "random_state": 42
}

log_rf(experimentID, "Second Run", params, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the UI to see how this appears.  Take a look at the artifact to see where the plot was saved.
# MAGIC 
# MAGIC Now, run a third run.

# COMMAND ----------

params_1000_trees = {
  "n_estimators": 1000,
  "max_depth": 10,
  "random_state": 42
}

log_rf(experimentID, "Third Run", params_1000_trees, X_train, X_test, y_train, y_test)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Querying Past Runs
# MAGIC 
# MAGIC You can query past runs programatically in order to use this data back in Python.  The pathway to doing this is an `MlflowClient` object.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can also set tags for runs using `client.set_tag(run.info.run_id, "tag_key", "tag_value")`

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC Now list all the runs for your experiment using `.list_run_infos()`, which takes your `experiment_id` as a parameter.

# COMMAND ----------

display(client.list_run_infos(experimentID))

# COMMAND ----------

# MAGIC %md
# MAGIC We can list the artifacts for any run by using the `MLflowClient().list_artifacts(run_id)` method:

# COMMAND ----------

client.list_artifacts(runID)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull out a few fields and create a pandas DataFrame with it.

# COMMAND ----------

runs = pd.DataFrame([(run.run_id, run.start_time, run.artifact_uri) for run in client.list_run_infos(experimentID)])
runs.columns = ["run_id", "start_time", "artifact_uri"]

display(runs)

# COMMAND ----------

# MAGIC %md
# MAGIC Pull the last run and take a look at the associated artifacts.

# COMMAND ----------

rf_run = runs.sort_values("start_time", ascending=False).iloc[2]
client.list_artifacts(rf_run["run_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC Return the evaluation metrics for the last run.

# COMMAND ----------

client.get_run(rf_run.run_id).data.metrics

# COMMAND ----------

# MAGIC %md
# MAGIC Reload the model and take a look at the feature importance.

# COMMAND ----------

import mlflow.sklearn

model = mlflow.sklearn.load_model(rf_run.artifact_uri+"/random-forest-model")
model.feature_importances_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC **Question:** What can MLflow Tracking log?
# MAGIC **Answer:** MLflow can log the following:
# MAGIC - **Parameters:** inputs to a model
# MAGIC - **Metrics:** the performance of the model
# MAGIC - **Artifacts:** any object including data, models, and images
# MAGIC - **Source:** the original code, including the commit hash if linked to git
# MAGIC 
# MAGIC **Question:** How do you log experiments?
# MAGIC **Answer:** Experiments are logged by first creating a run and using the logging methods on that run object (e.g. `run.log_param("MSE", .2)`).
# MAGIC 
# MAGIC **Question:** Where do logged artifacts get saved?
# MAGIC **Answer:** Logged artifacts are saved in a directory of your choosing.  On Databricks, this would be DBFS, or the Databricks File System, which backed by a blob store.
# MAGIC 
# MAGIC **Question:** How can I query past runs?
# MAGIC **Answer:** This can be done using an `MlflowClient` object.  This allows you do everything you can within the UI programatically so you never have to step outside of your programming environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Next Steps
# MAGIC 
# MAGIC Start the labs for this lesson, [Experiment Tracking Lab]($./Labs/02-Lab)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC **Q:** What is MLflow at a high level?
# MAGIC **A:** <a href="https://databricks.com/session/accelerating-the-machine-learning-lifecycle-with-mlflow-1-0" target="_blank">Listen to Spark and MLflow creator Matei Zaharia's talk at Spark Summit in 2019.</a>
# MAGIC 
# MAGIC **Q:** What is a good source for the larger context of machine learning tools?
# MAGIC **A:** <a href="https://roaringelephant.org/2019/06/18/episode-145-alex-zeltov-on-mlops-with-mlflow-kubeflow-and-other-tools-part-1/#more-1958" target="_blank">Check out this episode of the podcast Roaring Elephant.</a>
# MAGIC 
# MAGIC **Q:** Where can I find the MLflow docs?
# MAGIC **A:** <a href="https://www.mlflow.org/docs/latest/index.html" target="_blank">You can find the docs here.</a>
# MAGIC 
# MAGIC **Q:** What is a good general resource for machine learning?
# MAGIC **A:** <a href="https://www-bcf.usc.edu/~gareth/ISL/" target="_blank">_An Introduction to Statistical Learning_</a> is a good starting point for the themes and basic approaches to machine learning.
# MAGIC 
# MAGIC **Q:** Where can I find out more information on machine learning with Spark?
# MAGIC **A:** Check out the Databricks blog <a href="https://databricks.com/blog/category/engineering/machine-learning" target="_blank">dedicated to machine learning</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>