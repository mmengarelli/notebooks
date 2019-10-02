// Databricks notebook source
// MAGIC %md 
// MAGIC Index
// MAGIC * Amazon Reviews: /databricks-datasets/amazon/data20K
// MAGIC * /mnt/mikem/imdb_amazon_yelp.parquet
// MAGIC * [NTLK](https://www.nltk.org/book/ch02.html)
// MAGIC * /FileStore/tables/imdb_labelled.txt
// MAGIC * /mnt/wikipedia-readonly/en_wikipedia/
// MAGIC * /mnt/mikem/tmp/train-balanced-sarcasm.csv
// MAGIC * [Movie Reviews](http://ai.stanford.edu/~amaas/data/sentiment/)
// MAGIC * [Newsgroup Data](https://archive.ics.uci.edu/ml/machine-learning-databases/20newsgroups-mld/)
// MAGIC * [Yelp Reviews](https://www.yelp.com/dataset)
// MAGIC * [Wikipedia corpus](https://nlp.cs.nyu.edu/wikipedia-data/)
// MAGIC * [Enron Emails](https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz)
// MAGIC * /dbfs/mnt/mikem/datasets/reuters
// MAGIC * /home/silvio/enron-years
// MAGIC * /databricks-datasets/news20.binary/data-001/training

// COMMAND ----------

https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz

// COMMAND ----------

// MAGIC %sh tar -xvf enron_mail_20150507.tar.gz

// COMMAND ----------

// MAGIC %sh mv reuters/test /dbfs/mnt/mikem/datasets/reuters

// COMMAND ----------

// MAGIC %sh unzip /root/nltk_data/corpora/reuters.zip

// COMMAND ----------

// MAGIC %python
// MAGIC import nltk
// MAGIC 
// MAGIC from nltk.corpus import reuters
// MAGIC 
// MAGIC nltk.download('reuters')