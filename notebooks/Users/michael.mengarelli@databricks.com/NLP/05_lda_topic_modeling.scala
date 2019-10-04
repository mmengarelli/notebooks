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
// MAGIC <img src="http://uebercomputing.com/img/Logo_de_Enron.svg" style="height: 35px; padding: 10px"/>
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
corpus.cache().count()

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.functions._

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
val vectorizer = new CountVectorizer()
  .setInputCol(remover.getOutputCol)
  .setOutputCol("features")
  .setVocabSize(10000)
  .setMinDF(5)

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(tokenizer, remover, vectorizer))
val pipelineModel = pipeline.fit(corpus)

val preppedDocs = pipelineModel.transform(corpus)

// COMMAND ----------

val numTopics = 10
val lda = new LDA()
  .setOptimizer("em")
  .setK(numTopics)

// COMMAND ----------

val model = lda.fit(preppedDocs)

// COMMAND ----------

val vocab = pipelineModel.stages(2).asInstanceOf[org.apache.spark.ml.feature.CountVectorizerModel].vocabulary

// COMMAND ----------

import scala.collection.mutable.WrappedArray

val toWords = udf( (x : WrappedArray[Int]) => { x.map(i => vocab(i)) })

// COMMAND ----------

val topics = model.describeTopics(4).withColumn("topicWords", toWords(col("termIndices")))
val df = topics.select("topic", "topicWords")
display(df)