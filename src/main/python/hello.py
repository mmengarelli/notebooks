from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.range(1,100).toDF("id").createOrReplaceTempView("idz")

spark.sql("select * from idz").show()

