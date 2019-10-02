# Databricks notebook source
# MAGIC %md 
# MAGIC # PDF Extraction - NER
# MAGIC 
# MAGIC ### Goal
# MAGIC * Covert pdf-text
# MAGIC * Extract named entities
# MAGIC 
# MAGIC 
# MAGIC Requires
# MAGIC * pdfminer.six
# MAGIC 
# MAGIC <!--
# MAGIC # mm-demo
# MAGIC # demo-ready
# MAGIC # John Snow Labs spark-nlp
# MAGIC # Named Entity Recognition NER
# MAGIC -->

# COMMAND ----------

from io import BytesIO, StringIO

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams

def pdf2text(content):
    bytes = BytesIO(content)
    s = StringIO()
    resource = PDFResourceManager()
    device = TextConverter(resource, s, codec='utf-8', laparams=LAParams())
    interpreter = PDFPageInterpreter(resource, device)
    
    for page in PDFPage.get_pages(bytes):
        interpreter.process_page(page)

    text = s.getvalue()
    device.close()
    s.close()

    return text

# COMMAND ----------

# MAGIC %fs ls /mnt/mikem/data/nlp/pdf/

# COMMAND ----------

# MAGIC %md #### Convert to text

# COMMAND ----------

from pyspark.sql.types import *
from os.path import basename

schema = StructType([ \
  StructField("path", StringType(), False), \
  StructField("filename", StringType(), False), \
  StructField("text", StringType(), False)])

# COMMAND ----------

# DBTITLE 0,Convert to text
rdd = sc.binaryFiles("/mnt/mikem/data/nlp/pdf/").map(lambda e: (e[0], basename(e[0]), pdf2text(e[1])))
df = rdd.toDF(schema)

df.cache().count()

# COMMAND ----------

display(df.select("text"))

# COMMAND ----------

# MAGIC %md #### Extraction

# COMMAND ----------

import sparknlp 
from sparknlp.pretrained import PretrainedPipeline 

pipeline = PretrainedPipeline('recognize_entities_dl_noncontrib', 'en')
result = pipeline.annotate(df, column = 'text') 

# COMMAND ----------

result.cache().count()

# COMMAND ----------

from pyspark.sql.functions import explode, col
from wordcloud import WordCloud,STOPWORDS

import matplotlib.pyplot as plt

exploded = result.select(explode(col('entities.result')).alias("entities"))

l = list(exploded.toPandas()["entities"])
st = ''.join(str(e.encode('ascii','ignore')) for e in l)

wc = WordCloud(max_words=1000, width=640, height=480, margin=0, stopwords=STOPWORDS, colormap='viridis').generate(st)
 
plt.imshow(wc, interpolation='bilinear')
plt.axis("off")
plt.margins(x=0, y=0)
display(plt.show())

# COMMAND ----------

