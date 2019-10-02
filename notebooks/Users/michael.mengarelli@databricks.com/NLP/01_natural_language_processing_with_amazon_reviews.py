# Databricks notebook source
# MAGIC %md # Natural Language Processing with Amazon Reviews
# MAGIC 
# MAGIC ### Goals of the Demo
# MAGIC * Explore **Spark MLlib's** feature extraction and transformation capabilities 
# MAGIC * Train a **Binary Classification Model** to predict setntiment of product reviews
# MAGIC * Deploy the trained model and score in real time
# MAGIC 
# MAGIC <!-- 
# MAGIC #demo-ready
# MAGIC #mm-demo
# MAGIC -->

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %scala
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.ml.PipelineModel
# MAGIC import org.apache.spark.sql.streaming.StreamingQuery
# MAGIC val streamSchema = new StructType()
# MAGIC   .add(StructField("rating",DoubleType,true))
# MAGIC   .add(StructField("review",StringType,true))
# MAGIC   .add(StructField("time",LongType,true))
# MAGIC   .add(StructField("title",StringType,true))
# MAGIC   .add(StructField("user",StringType,true))

# COMMAND ----------

# DBTITLE 1,Examine the training data
data = spark.read.parquet("/databricks-datasets/amazon/data20K")
data.createOrReplaceTempView("reviews")
display(data)

# COMMAND ----------

data.count()

# COMMAND ----------

# MAGIC %sql SELECT count(1), rating FROM reviews GROUP BY rating ORDER BY rating

# COMMAND ----------

# MAGIC %md ### Featurization

# COMMAND ----------

# DBTITLE 0,NLP Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer

tokenizer = RegexTokenizer()    \
  .setInputCol("review")        \
  .setOutputCol("tokens")       \
  .setPattern("\\W+")

remover = StopWordsRemover()    \
  .setInputCol("tokens")        \
  .setOutputCol("stopWordFree") \

counts = CountVectorizer()      \
  .setInputCol("stopWordFree")  \
  .setOutputCol("features")     \
  .setVocabSize(1000)

# COMMAND ----------

from pyspark.ml.feature import Binarizer

# Covnert to 0/1
binarizer = Binarizer()  \
  .setInputCol("rating") \
  .setOutputCol("label") \
  .setThreshold(3.5)

# COMMAND ----------

# MAGIC %md ### Workflows with Pyspark.ML Pipeline
# MAGIC <img src="https://s3-us-west-2.amazonaws.com/pub-tc/ML-workflow.png" width="640">

# COMMAND ----------

# MAGIC %md ### Train Classifier 

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

lr = LogisticRegression()

p = Pipeline().setStages([tokenizer, remover, counts, binarizer, lr])

# COMMAND ----------

# DBTITLE 0,Model Training
splits = data.randomSplit([0.8, 0.2], 42)
train = splits[0].cache()
test = splits[1].cache()

# COMMAND ----------

model = p.fit(train)
model.stages[-1].summary.areaUnderROC

# COMMAND ----------

# DBTITLE 1,Validate
result = model.transform(test)

# COMMAND ----------

# DBTITLE 1,Examine results
display(result.select("rating", "label", "prediction", "review"))

result.createOrReplaceTempView("result")

# COMMAND ----------

# DBTITLE 1,Model evaluation
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator()
print("AUC: %(result)s" % {"result": evaluator.evaluate(result)})

# COMMAND ----------

partialPipeline = Pipeline().setStages([tokenizer, remover, counts, binarizer])
preppedData = partialPipeline.fit(train).transform(train)
lrModel = LogisticRegression().fit(preppedData)
display(lrModel, preppedData, "ROC")

# COMMAND ----------

print(lr.explainParams())

# COMMAND ----------

lr.setRegParam(0.01)
lr.setElasticNetParam(0.1)
counts.setVocabSize(1000)
model = p.fit(train)
result = model.transform(test)
print("AUC %(result)s" % {"result": BinaryClassificationEvaluator().evaluate(result)})

# COMMAND ----------

# DBTITLE 1,Serialize model
model.write().overwrite().save("/mnt/mikem/models/amazon-model")

# COMMAND ----------

# MAGIC %fs ls /mnt/mikem/models/amazon-model/

# COMMAND ----------

# MAGIC %md ### Real-time scoring

# COMMAND ----------

# MAGIC %scala
# MAGIC val inputStream = spark
# MAGIC   .readStream
# MAGIC   .schema(streamSchema)
# MAGIC   .option("maxFilesPerTrigger", 1)
# MAGIC   .json("/mnt/mikem/demo-data/amazon-stream-input/")

# COMMAND ----------

# DBTITLE 1,Score
# MAGIC %scala
# MAGIC // Load serialized model from disk
# MAGIC val model = PipelineModel.load("/mnt/databricks-myles/models/amazon-model")
# MAGIC 
# MAGIC // Score
# MAGIC val scoredStream = model.transform(inputStream)
# MAGIC 
# MAGIC scoredStream.writeStream
# MAGIC   .format("memory")
# MAGIC   .queryName("stream")
# MAGIC   .start()

# COMMAND ----------

# MAGIC %sql select rating, label, prediction, review from stream

# COMMAND ----------

# DBTITLE 1,Monitoring
# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC val time = (text: String) => {
# MAGIC   new java.sql.Timestamp(text.toLong*1000)
# MAGIC }
# MAGIC val timeUdf = udf(time)
# MAGIC 
# MAGIC val isMatch = (pred: Double, label: Double) => if(label==pred) "yes" else "no"
# MAGIC val isMatchUdf = udf(isMatch)
# MAGIC 
# MAGIC display(scoredStream
# MAGIC   .withColumn("time_col", timeUdf($"time") )
# MAGIC   .withColumn("is_match", isMatchUdf($"prediction", $"label") )
# MAGIC   .groupBy($"is_match", window($"time_col", "1 hour"))
# MAGIC   .count()
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.streams.active.foreach((s: StreamingQuery) => s.stop())