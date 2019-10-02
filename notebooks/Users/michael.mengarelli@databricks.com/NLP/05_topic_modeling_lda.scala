// Databricks notebook source
// MAGIC %md-sandbox 
// MAGIC # Topic Modeling using LDA
// MAGIC Model that determines the abstract topics that occur in a collection of documents
// MAGIC Text mining tool used for discovering hidden semantics structures in a body of text 
// MAGIC 
// MAGIC We will use LDA [Latent Dirichlet Allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation) to discover topics in email documents.
// MAGIC 
// MAGIC ### Steps
// MAGIC 1. Prepare our data for topic extraction 
// MAGIC 2. Train a LDA model
// MAGIC 3. Evaluate results
// MAGIC 
// MAGIC ### Dataset
// MAGIC <img src="http://uebercomputing.com/img/Logo_de_Enron.svg" style="height: 35px; border: 1px solid #ddd; border-radius: 2px 2px 2px 2px; padding: 10px"/>
// MAGIC [Enron emails](https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz) publicly available dataset
// MAGIC 
// MAGIC <!--
// MAGIC # Enron
// MAGIC # LDA Topic Modeling
// MAGIC # mm-demo
// MAGIC # demo-ready
// MAGIC -->

// COMMAND ----------

val corpus = spark.read.parquet("/home/silvio/enron-years")
corpus.count()

// COMMAND ----------

display(corpus.limit(5).select("body"))

// COMMAND ----------

// MAGIC %md #### Feature Extraction

// COMMAND ----------

val moreStopWords = Array(
 "enron", "forward", "attached", "attach", "http", "[image]", "mail", 
 "said", "message", "mailto", "href", "subject", "corp", "email", "font",
 "thank", "thanks", "sent", "html", "table", "width", "height", "image", 
 "nbsp", "size", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 
 "forwarded"
)

// COMMAND ----------

import org.apache.spark.ml.feature._

val tokenizer = new RegexTokenizer()
  .setInputCol("body")
  .setOutputCol("words")
  .setPattern("[A-z]+")
  .setGaps(false)
  .setMinTokenLength(4)

val stopWords = sc.textFile("/home/silvio/stopwords.txt").collect().union(moreStopWords)
val remover = new StopWordsRemover()
  .setStopWords(stopWords)
  .setInputCol(tokenizer.getOutputCol)
  .setOutputCol("noStopWords")

// LDA takes in a vector of token counts as input
val countVectorizer = new CountVectorizer()
  .setInputCol(remover.getOutputCol)
  .setOutputCol("features")
  .setVocabSize(10000)
  .setMinDF(5)

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline()
pipeline.setStages(Array(
  tokenizer, 
  remover, 
  countVectorizer))

val featurizedModel = pipeline.fit(corpus)

val featureDF = featurizedModel.transform(corpus)

// COMMAND ----------

display(featureDF)

// COMMAND ----------

// MAGIC %md #### Fit LDA

// COMMAND ----------

import org.apache.spark.ml.clustering.LDA

val numTopics = 10
val lda = new LDA()
  .setOptimizer("em")
  .setK(numTopics)
  .setMaxIter(20) 

val model = lda.fit(featureDF)

// COMMAND ----------

// This is always negative, and closer to 0 is better
val ll = model.logLikelihood(featureDF)
val lp = model.logPerplexity(featureDF)

// COMMAND ----------

val result = model.transform(featureDF)

//result.printSchema
//display(result)
