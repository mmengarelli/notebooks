# Databricks notebook source
# MAGIC %sh ls ../helper

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath('../helper'))

from foobar import *

# COMMAND ----------

customers_bronze_clean_v()
