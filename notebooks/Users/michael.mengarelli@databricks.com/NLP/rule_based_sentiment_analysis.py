# Databricks notebook source
# MAGIC %md <img src="https://nlp.johnsnowlabs.com/assets/images/logo.png" width="180" height="50" style="float: left;">

# COMMAND ----------

# MAGIC %md ## Rule-based Sentiment Analysis
# MAGIC 
# MAGIC In the following example, we walk-through a simple use case for our straight forward SentimentDetector annotator.
# MAGIC 
# MAGIC This annotator will work on top of a list of labeled sentences which can have any of the following features
# MAGIC     
# MAGIC     positive
# MAGIC     negative
# MAGIC     revert
# MAGIC     increment
# MAGIC     decrement
# MAGIC 
# MAGIC Each of these sentences will be used for giving a score to text 

# COMMAND ----------

# MAGIC %md #### 1. Call necessary imports and set the resource path to read local data files

# COMMAND ----------

#Imports
import sys
sys.path.append('../../')

import sparknlp

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import array_contains
from sparknlp.annotator import *
from sparknlp.common import RegexRule
from sparknlp.base import DocumentAssembler, Finisher

# COMMAND ----------

# MAGIC %md #### 2. Load SparkSession if not already there

# COMMAND ----------

import sparknlp 

spark = sparknlp.start()

print("Spark NLP version")
sparknlp.version()
print("Apache Spark version")
spark.version

# COMMAND ----------

! rm /tmp/sentiment.parquet.zip
! rm -rf /tmp/sentiment.parquet
! wget -N https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/resources/en/sentiment.parquet.zip -P /tmp
! wget -N https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/resources/en/lemma-corpus-small/lemmas_small.txt -P /tmp
! wget -N https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/resources/en/sentiment-corpus/default-sentiment-dict.txt -P /tmp    

# COMMAND ----------

! unzip /tmp/sentiment.parquet.zip -d /tmp/

# COMMAND ----------

data = spark. \
        read. \
        parquet("/tmp/sentiment.parquet"). \
        limit(10000).cache()

data.show()

# COMMAND ----------

# MAGIC %md #### 3. Create appropriate annotators. We are using Sentence Detection, Tokenizing the sentences, and find the lemmas of those tokens. The Finisher will only output the Sentiment.

# COMMAND ----------

document_assembler = DocumentAssembler() \
    .setInputCol("text")

sentence_detector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

tokenizer = Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token")

lemmatizer = Lemmatizer() \
    .setInputCols(["token"]) \
    .setOutputCol("lemma") \
    .setDictionary("/tmp/lemmas_small.txt", key_delimiter="->", value_delimiter="\t")
        
sentiment_detector = SentimentDetector() \
    .setInputCols(["lemma", "sentence"]) \
    .setOutputCol("sentiment_score") \
    .setDictionary("/tmp/default-sentiment-dict.txt", ",")
    
finisher = Finisher() \
    .setInputCols(["sentiment_score"]) \
    .setOutputCols(["sentiment"])

# COMMAND ----------

# MAGIC %md #### 4. Train the pipeline, which is only being trained from external resources, not from the dataset we pass on. The prediction runs on the target dataset

# COMMAND ----------

pipeline = Pipeline(stages=[document_assembler, sentence_detector, tokenizer, lemmatizer, sentiment_detector, finisher])
model = pipeline.fit(data)
result = model.transform(data)

# COMMAND ----------

# MAGIC %md #### 5. filter the finisher output, to find the positive sentiment lines

# COMMAND ----------

result.where(array_contains(result.sentiment, "positive")).show(10,False)

# COMMAND ----------

