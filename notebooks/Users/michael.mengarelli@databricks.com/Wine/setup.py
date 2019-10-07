# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml import Pipeline
from pyspark.sql.functions import expr
from pyspark.sql.functions import col
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
import matplotlib.pyplot as plt

schema = StructType([
  StructField("country", StringType(), True),
  StructField("description", StringType(), True),
  StructField("designation", StringType(), True),
  StructField("points", IntegerType(), True),
  StructField("price", IntegerType(), True),
  StructField("province", StringType(), True),
  StructField("region_1", StringType(), True),
  StructField("region_2", StringType(), True),
  StructField("taster_name", StringType(), True),
  StructField("taster_twitter_handle", StringType(), True),
  StructField("title", StringType(), True),
  StructField("variety", StringType(), True),
  StructField("winery", StringType(), True)])

None