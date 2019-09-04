// Databricks notebook source
spark.range(1,10).write.format("delta").saveAsTable("mikem.test")

// COMMAND ----------

val jsonStr1 = """
{"metadata": {"key": 84896, "value": 54, "stage": "one" },
"sub1": {"key":1, "first":"Joe", "raw_id":"N/A", "sub2": {"cust_id":9999, "cust_type":"weekly", "last_order": "2019-01-01"}},
"key": 100
}"""

val jsonStr2 = """
{"metadata": { "key": 84896, "value": 154, "hop": "two"}, 
"sub1": {"key":2, "first":"king", "last":"george", "raw_id":"1.234", "sub2": {"cust_id":1234, "cust_zip":900120, "cust_type":"monthly"}},
"key": "200"
}"""

val dfTst1 = spark.read.json(Seq(jsonStr1).toDS)
val dfTst2 = spark.read.json(Seq(jsonStr2).toDS)

dfTst1.printSchema()
dfTst2.printSchema()

/** need some magic structType merge here **/
val unionDf = dfTst1.union(dfTst2)

// COMMAND ----------

import org.apache.spark.sql.types.StructType

val st1 = dfTst1.schema
val st2 = dfTst2.schema

val uSt1 = st1.union(st2)
val st3 = StructType(uSt1)

println(s"new schema = \n${st1.treeString}")

// COMMAND ----------

I was hoping `org.apache.spark.sql.types.StructType.union` would work, e.g.:
```import org.apache.spark.sql.types.StructType
val st1 = dfTst1.schema
val st2 = dfTst2.schema
val uSt1 = st1.union(st2)
val st3 = StructType(uSt1)
println(s"new schema = \n${st1.treeString}")```
but that function doesn't work recursively in the nested Structs
I also see a very cool looking `StructType.merge` function  :grinning:
... but it's marked `private`  :disappointed:
I think I'm having the same question as this person:  http://apache-spark-user-list.1001560.n3.nabble.com/Why-the-merge-method-in-StructType-is-private-td29939.html

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO mikem.deleteme a
// MAGIC USING mikem.deleteme b
// MAGIC ON  a.id = b.id 
// MAGIC WHEN MATCHED THEN DELETE *
// MAGIC WHEN NOT MATCHED THEN INSERT *

// COMMAND ----------

// MAGIC %sql desc mikem.deleteme

// COMMAND ----------

// MAGIC %sql create table mikem 
// MAGIC id long
// MAGIC using delta
// MAGIC location "/test"

// COMMAND ----------

spark.range(0,100).write.parquet("/test")

// COMMAND ----------

val r00 = sc.parallelize(0 to 9)
val r01 = sc.parallelize(0 to 90 by 10)
val r10 = r00 cartesian r01
val r11 = r00.map(n => (n, n))
val r12 = r00 zip r01
val r13 = r01.keyBy(_ / 20)
val r20 = Seq(r11, r12, r13).foldLeft(r10)(_ union _)

// COMMAND ----------

// DBTITLE 1,Lineage
r20.toDebugString

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/delta/delayed_flights_delta/

// COMMAND ----------

// MAGIC %sql desc mikem.airline

// COMMAND ----------

// DBTITLE 1,update a jar on all nodes
import sys.process._

val library = "jars/8ff17b58_e3a0_427e_a5da_b7b5dc93c9dc-spark_sql_2_11_2_3_0_SNAPSHOT-aca38.jar"
val cmd = s"cp /dbfs/FileStore/$library /databricks/jars/spark--versions--2.2--sql_2.11_deploy.jar"
spark.range(1, 100).foreachPartition { _ => cmd.!! }

"killall java".!!

// COMMAND ----------

def getRowVal(row:Row) : String = row.toSeq.toList(0).asInstanceOf[String]

// init dataframe to store results
var df = Seq(("","","")).toDF("Database","Table","Provider")

val databases = spark.catalog.listDatabases().select("name").collect.map(_.toSeq.toList)
for(i <- 0 until databases.length){
  val dbName = databases(i)(0).asInstanceOf[String]

  val tables = spark.catalog.listTables(dbName).select("name").collect.map(_.toSeq.toList)
  for(j <- 0 until tables.length){
    val tableName = tables(j)(0).asInstanceOf[String]
   
    val provider = spark.sql(s"desc extended $dbName.$tableName").where("col_name = 'Provider'").collect.map(_.toSeq.toList(1)).mkString
    df = df.union(Seq((dbName,tableName,provider)).toDF("Database","Table","Provider"))
  }  
}
display(df.where("Provider = 'DELTA'"))

// COMMAND ----------

var df = 
display(df)

// COMMAND ----------

// MAGIC %sql CONVERT TO DELTA parquet.`path/to/table` PARTITIONED BY (date string)

// COMMAND ----------

// MAGIC %sql CONVERT TO DELTA parquet.`/mnt/quickfire-analytics/test/recsrate201902/` PARTITIONED BY(date)

// COMMAND ----------

spark.range(1,100000).toDF("testcol")


// COMMAND ----------

val df = spark.sql("select count(*), dayofweek from mikem.airline group by dayofweek")
display(df)

// COMMAND ----------

val df = spark.range(1,1000)
df.createOrReplaceTempView("df")

// COMMAND ----------

spark.catalog.listFunctions.filter('name like "%avro%").show(false)

// COMMAND ----------

// MAGIC %fs ls s3n://mikem-rs-temp/jason/redshift

// COMMAND ----------

val contents = """
print('\nExecuting SQL\n')

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
spark.sql("show databases").show()

df = spark.table('mikem.delayed_flights_with_location')
print(df)
print('\nDone\n')
"""

dbutils.fs.put("/mnt/mikem/test.py", contents, true)


// COMMAND ----------

val tb = spark.table("mikem.delayed_flights_with_location")
display(tb)

// COMMAND ----------

val df = spark.table("mikem.delayed_flights")
val alo = spark.table("mikem.airport_locations").select("latitude_deg", "longitude_deg","iata_code","iso_country","municipality","state")
.withColumnRenamed("iata_code", "origin_iata_code")
.withColumnRenamed("iso_country", "origin_iso_country")
.withColumnRenamed("municipality", "origin_city")
.withColumnRenamed("state", "origin_state")
.withColumnRenamed("longitude_deg", "origin_longitude")
.withColumnRenamed("latitude_deg", "origin_latitude")

val ald = spark.table("mikem.airport_locations").select("latitude_deg", "longitude_deg","iata_code","iso_country","municipality","state")
.withColumnRenamed("iata_code", "dest_iata_code")
.withColumnRenamed("iso_country", "dest_iso_country")
.withColumnRenamed("municipality", "dest_city")
.withColumnRenamed("state", "dest_state")
.withColumnRenamed("longitude_deg", "dest_longitude")
.withColumnRenamed("latitude_deg", "dest_latitude")

val res = df.join(alo, df("origin") === alo("origin_iata_code"))
val daFinal = res.join(ald, res("dest") === ald("dest_iata_code")).drop("origin_iata_code").drop("dest_iata_code")
//display(daFinal)
daFinal.write.format("parquet").mode("overwrite").option("path", "/mnt/mikem/airline/delayed_flights_with_location").saveAsTable("mikem.delayed_flights_with_location")

// COMMAND ----------

// MAGIC %sql 
// MAGIC SET spark.databricks.delta.formatCheck.enabled=false;
// MAGIC select * from parquet.`dbfs:/tmp/delayed_flights_delta/`

// COMMAND ----------

spark.range(100).toDF.show()

// COMMAND ----------

// MAGIC %sql show create table mikem.delayed_flights

// COMMAND ----------

spark.range(1,100000).toDF("id").write.parquet("/mnt/mikem/test-long-table")

// COMMAND ----------

// MAGIC %sql desc mikem.testd 

// COMMAND ----------

// MAGIC %sql
// MAGIC create table mikem.testd 
// MAGIC using DELTA 
// MAGIC location '/mnt/mikem/test-long-table'

// COMMAND ----------

// MAGIC %sql 
// MAGIC CONVERT TO DELTA parquet.`/mnt/mikem/test-long-table`

// COMMAND ----------

// MAGIC %sql create table mikem.testd using delta 
// MAGIC path `/mnt/mikem/test-long-table`

// COMMAND ----------

// MAGIC %sql alter table aapl_delta_timetravel2 rename to aapl_delta_timetravel

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) as flts, count(depdelay) as delays
// MAGIC from mikem.delayed_flights 
// MAGIC group by uniquecarrier

// COMMAND ----------

// MAGIC %sql select (arrdelay + depdelay + carrierdelay + weatherdelay + nasdelay + securitydelay) as totaldelay 
// MAGIC from mikem.delayed_flights

// COMMAND ----------

// MAGIC %sql select * from mikem.delayed_flights

// COMMAND ----------

val df = (1 to 2).toDF.saveAsTable()

// COMMAND ----------

spark.conf.get("spark.sql.warehouse.dir")

// COMMAND ----------

udo echo "" >> /databricks/spark/conf/spark-defaults.conf

// COMMAND ----------

// MAGIC %sql describe extended mikem.gumgumusage

// COMMAND ----------

// MAGIC %fs ls dbfs:/mnt/mikem/wikipedia-readonly/pageviews/	

// COMMAND ----------

// MAGIC %sql select * from aapl_delta as of '2018-12-10'

// COMMAND ----------

// MAGIC %sql select * from sqldfall_delta AS OF '2018-12-11'

// COMMAND ----------

// MAGIC %sql describe extended sqldfall_delta

// COMMAND ----------

// MAGIC %sql use mikem;
// MAGIC show tables;

// COMMAND ----------

// MAGIC %sql select * from mikem.aapl_delta

// COMMAND ----------

val r00 = sc.parallelize(0 to 9)
val r01 = sc.parallelize(0 to 90 by 10)
val r10 = r00 cartesian r01
val r11 = r00.map(n => (n, n))
val r12 = r00 zip r01
val r13 = r01.keyBy(_ / 20)
val r20 = Seq(r11, r12, r13).foldLeft(r10)(_ union _)

// COMMAND ----------

def getRowVal(row:Row) : String = row.toSeq.toList(0).asInstanceOf[String]

// init dataframe to store results
var df = sc.parallelize(List(("","",""))).toDF("Database","Table","Creator")

val databases = spark.catalog.listDatabases().select("name").collect.map(_.toSeq.toList)
for(i <- 0 until databases.length){
  val dbName = databases(i)(0).asInstanceOf[String]

  val tables = spark.catalog.listTables(dbName).select("name").collect.map(_.toSeq.toList)
  for(j <- 0 until tables.length){
    val tableName = tables(j)(0).asInstanceOf[String]
   
    val creator = spark.sql(s"desc history $dbName.$tableName").select("userName").where("operation like 'CREATE%'").collect.map(_.toSeq.toList(0)).mkString
    df = df.union(sc.parallelize(List(dbName,tableName,creator)).toDF())
  }  
}
df.na.drop()
display(df)

// COMMAND ----------

// DBTITLE 1,Keep
//Print table names in DB
def printTables(dbName:String) = {
  spark.catalog.listTables(dbName).select("name").collect.map(t => println(dbName+ "." +getRowVal(t)))
}

//Get string value out of row object
def getRowVal(row:Row) : String = row.toSeq.toList(0).asInstanceOf[String]

spark.catalog.listDatabases.select("name").collect.map(db => printTables(getRowVal(db)))

// COMMAND ----------

import org.apache.spark.sql.types.StringType
//Loop thru db's and set dbname
val dbName = "mikem"
val df = spark.catalog.listTables(dbName).select("name")
val names = df.collect.map(_.toSeq.toList)
val ct = df.count().asInstanceOf[Int]

var next = ""
var sql = ""
for(i <- 1 until ct){ 
  next = names(i)(0).asInstanceOf[String]
  sql = s"select * from $dbName.$next limit 1"
  println(s"$sql")
  spark.sql(s"$sql")
}

// COMMAND ----------

spark.conf.getAll.foreach(println _)

// COMMAND ----------

spark.shuffle.io.serverThreads = 36
by default it is 5

spark.network.timeout 240s
spark.stage.maxConsecutiveAttempts 10
spark.driver.extraJavaOptions -Djava.io.tmpdir=/local_disk0 
spark.databricks.delta.snapshotPartitions 1024 
spark.shuffle.io.serverThreads 32 
spark.databricks.queryWatchdog.enabled false 
spark.cleaner.referenceTracking.blocking false 
spark.scheduler.listenerbus.eventqueue.capacity 210000 
spark.databricks.preemption.enabled false 
spark.databricks.io.cache.enabled false 
spark.sql.shuffle.partitions 2048 
spark.hadoop.fs.s3a.multipart.threshold = 204857600 
spark.cleaner.periodicGC.interval 15min 
spark.executor.extraJavaOptions -Djava.io.tmpdir=/local_disk0 

spark.hadoop.fs.s3a.multipart.size = 104857600
spark.sql.autoBroadcastJoinThreshold 1000000
spark.driver.maxResultSize 20g
spark.shuffle.service.enabled false
spark.network.timeout 600s
spark.rpc.askTimeout 600s
spark.executor.memory 12g
spark.dynamicAllocation.enabled false
spark.sql.shuffle.partitions 950

// COMMAND ----------

spark.conf.set("spark.shuffle.io.serverThreads", 36)
spark.conf.set("spark.network.timeout", "240s")

// COMMAND ----------

//spark.conf.get("spark.shuffle.io.serverThreads")
//spark.conf.get("spark.network.timeout")
spark.conf.get("spark.executor.heartbeatInterval")


// COMMAND ----------

import org.apache.spark.util.SizeEstimator

val df = spark.table("aapl_delta")

println(SizeEstimator.estimate(df))


// COMMAND ----------

dbutils.fs.put("dbfs:/mnt/mikem/test.py", """
print 'hello world'
spark.sql("select * from default.aapl_delta")
""")

// COMMAND ----------

// MAGIC %python
// MAGIC from bokeh.plotting import figure
// MAGIC from bokeh.embed import components, file_html
// MAGIC from bokeh.resources import CDN
// MAGIC 
// MAGIC # prepare some data
// MAGIC x = [1, 2, 3, 4, 5]
// MAGIC y = [6, 7, 2, 4, 5]
// MAGIC 
// MAGIC # create a new plot with a title and axis labels
// MAGIC p = figure(title="simple line example", x_axis_label='x', y_axis_label='y')
// MAGIC 
// MAGIC # add a line renderer with legend and line thickness
// MAGIC p.line(x, y, legend="Temp.", line_width=2)
// MAGIC 
// MAGIC # create an html document that embeds the Bokeh plot
// MAGIC html = file_html(p, CDN, "my plot1")
// MAGIC 
// MAGIC # display this html
// MAGIC displayHTML(html)

// COMMAND ----------

// MAGIC %sh  /databricks/python/bin/python  - <<EOF
// MAGIC print "Start python init script..."
// MAGIC import urllib2
// MAGIC import json
// MAGIC import boto.ec2
// MAGIC EOF

// COMMAND ----------

// MAGIC %python import boto.ec2

// COMMAND ----------

// MAGIC %python 
// MAGIC import urllib2
// MAGIC import json
// MAGIC import boto.ec2
// MAGIC 
// MAGIC instance = json.loads(urllib2.urlopen('http://169.254.169.254/latest/dynamic/instance-identity/document').read())
// MAGIC SPARK_MASTER_HOSTNAME = urllib2.urlopen('http://169.254.169.254/latest/meta-data/public-hostname').read()
// MAGIC SPARK_MASTER_HOSTNAME
// MAGIC                                         
// MAGIC ACCESS_KEY = "YOUR_KEY"
// MAGIC SECRET_KEY = "YOUR_SECRET_KEY"
// MAGIC EIP_ADDRESS = "ELASTIC_IPADDRESS"
// MAGIC 
// MAGIC conn = boto.ec2.connect_to_region(instance['region'], aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
// MAGIC address = conn.get_all_addresses(filters={'public_ip': EIP_ADDRESS})[0]
// MAGIC address
// MAGIC #address.association_id
// MAGIC #conn.associate_address(instance_id=instance[u'instanceId'], allocation_id=address.allocation_id)

// COMMAND ----------

// MAGIC %python 
// MAGIC import boto3
// MAGIC 
// MAGIC response = boto3.client('sts').get_caller_identity()
// MAGIC 
// MAGIC print response

// COMMAND ----------

// MAGIC %scala 
// MAGIC spark.conf.set("spark.databricks.delta.formatCheck.enabled",false)
// MAGIC spark.conf.set("spark.databricks.io.cache.enabled",false)
// MAGIC spark.conf.set("spark.databricks.io.parquet.nativeReader.enabled",false)
// MAGIC spark.conf.set("spark.databricks.io.parquet.fastreader.enabled",false)
// MAGIC spark.conf.set("spark.sql.parquet.enableVectorizedReader",false)
// MAGIC 
// MAGIC spark.read.parquet("/mnt/delta/chat_receivemessage_2018/date=2018-10-16/part-00571-8f38459e-644c-4737-abbd-48f69dd2c0f1.c000.snappy.parquet").foreach { row => row.toString }

// COMMAND ----------

spark.range(1).map{ x => val x = new org.apache.spark.io.SnappyCompressionCodec(null); x.version }.show

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC df = (spark.read 
// MAGIC   .option("inferSchema", "true") 
// MAGIC   .option("header", "true")
// MAGIC   .csv("/mnt/mikem/AAPL.csv")
// MAGIC   .drop("Adj Close"))
// MAGIC   
// MAGIC df.write.format("delta").save("/tmp/mikem/dummy_delta_test")

// COMMAND ----------

table("mikem.airline")

// COMMAND ----------

"spark.sql("""
  CREATE TABLE aapl_delta 
  USING DELTA 
  LOCATION '{}' 
""".format(deltaDataPath))


// COMMAND ----------

sc.setLocalProperty("spark.scheduler.pool", "pool2")

import org.apache.spark.sql.functions._
sc.parallelize(1 to 10).toDF().repartition(400).agg(sum('value)).show() 



// COMMAND ----------

dbutils.fs.put("dbfs:/databricks/init/mikem/configureSchedulerPools.sh", """#!/usr/bin/bash
xml='<?xml version="1.0"?>
<allocations>
  <pool name="pool1">
    <schedulingMode>FIFO</schedulingMode>
    <weight>1</weight>
    <minShare>2</minShare>
  </pool>
  <pool name="pool2">
    <schedulingMode>FIFO</schedulingMode>
    <weight>10</weight>
    <minShare>3</minShare>
  </pool>
</allocations>
'
echo "$xml" >> /databricks/hive/conf/fairscheduler.xml
""")

// COMMAND ----------

val user = dbutils.secrets.get(scope="test", key="user")
val pass = dbutils.secrets.get(scope="test", key="pass")

// COMMAND ----------

dbutils.fs.ls("s3a://mikem-docs/data/akc_breed_info.csv")

// COMMAND ----------

// MAGIC %scala 
// MAGIC dbutils.fs.put("/databricks/init/mikem/test.sh", s, true)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/mikem/tmp/deleteme1"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://source@adbworkshops.blob.core.windows.net/",
  mount_point = "/mnt/mikem/training-sources/",
  extra_configs = {"fs.azure.sas.source.adbworkshops.blob.core.windows.net": "?sv=2017-07-29&ss=bfqt&srt=sco&sp=rl&se=2025-08-01T09:42:35Z&st=2018-02-19T02:42:35Z&spr=https&sig=wjB6JWOBwLsxNv9udmaMq6S3FbzfS2YhBIiHf7nW%2F3M%3D"})

// COMMAND ----------

// DBTITLE 1,Filestore
dbutils.fs.put("/FileStore/mikem/docs/test.html", "<p>Hello World</p>")

// COMMAND ----------

// MAGIC %sh nc -zv auora-test-cluster.cluster-chlee7xx28jo.us-west-2.rds.amazonaws.com 3306

// COMMAND ----------

import org.apache.spark.sql.functions._

spark.range(5).withColumn("date", lit("2017-00-00"))
   .select(to_date(col("date")))
   .show(1)

// COMMAND ----------

// MAGIC %sh 
// MAGIC echo $AWS_ACCESS_KEY_ID
// MAGIC echo $AWS_SECRET_ACCESS_KEY

// COMMAND ----------

dbutils.environment.getPassword("pass")

// COMMAND ----------

import java.util.Properties

val host = "databricks-customer-example.chlee7xx28jo.us-west-2.rds.amazonaws.com"
val port = 3306
val database = "databricks"
val user = "root"
val pass = "tMFqirgjz1lc"
val driver = "com.mysql.cj.jdbc.Driver"
val url = s"jdbc:mysql://${host}:${port}/${database}?user=${user}&password=${pass}"
val sql = "select * from databricks.miketest"

val connectionProperties = new Properties()
connectionProperties.put("user", s"${user}")
connectionProperties.put("password", s"${pass}")

// COMMAND ----------

val q = """
(SELECT id, 
CASE
    WHEN MONTH(start_date) = 0 THEN NULL
    WHEN DAY(start_date) = 0 THEN NULL

ELSE start_date
END
FROM miketest) tb_alias
"""

val df = spark.read.jdbc(url=url, table=q, properties=connectionProperties).show

// COMMAND ----------

val df = spark.read 
.format("jdbc") 
.schema(schema)
.option("driver", driver)
.option("url", url)
.option("dbtable", "databricks.miketest") 
.load()

df.write.parquet("/mnt/mikem/tmp/miketest")


// COMMAND ----------

var success:Boolean = false
try {
 val df = sqlContext.read 
  .format("jdbc") 
  .option("driver", driver)
  .option("url", url)
  .option("dbtable", "databricks.miketest") 
  .load()
  
  success = true
} catch {
    case _: Throwable => println("exception ignored")
}

println(success)


// COMMAND ----------

val props = new java.util.Properties()
props.setProperty("Driver", driver)
props.setProperty("fetchSize", "10000")

val df = spark.read.jdbc(
  url=url,
  table="databricks.members",
  columnName="member_id",
  lowerBound=1l,
  upperBound=10000L,
  numPartitions=10,
  connectionProperties=props)

display(df)

// COMMAND ----------

val df = sqlContext.read 
  .format("jdbc") 
  .option("driver", driver)
  .option("url", url)
  .option("lowerBound", 1L) 
  .option("upperBound", 10000L) 
  .option("partitionColumn", "member_id") 
  .option("numPartitions", 10) 
  .option("fetchSize", 10000) 
  .option("dbtable", "databricks.members") 
  .load()

df.show

// COMMAND ----------

spark.sql(
 """
  select * from databricks.members
  USING org.apache.spark.sql.jdbc
  OPTIONS (url '$url', dbtable 'databricks.members', user '$user', password '$pass',
  partitionColumn '"member_id"', lowerBound '100000', upperBound '400000', numPartitions '10')
  """)

// COMMAND ----------

val df1 = (1 to 3).toDF()
val df2 = (4 to 6).toDF()

val cDf = df1.crossJoin(df2)
cDf.explain(true)


// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*), ss_wholesale_cost from  store_sales group by ss_wholesale_cost

// COMMAND ----------

// MAGIC %python 
// MAGIC pd_df = spark.range(1,100).toPandas()
// MAGIC pd_df

// COMMAND ----------

display(spark.catalog.listTables)

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.range(10)
// MAGIC df.createOrReplaceTempView("test")
// MAGIC 
// MAGIC def get_req(s):
// MAGIC   import requests
// MAGIC   return requests.get("https://www.google.com").content
// MAGIC 
// MAGIC spark.udf.register("getReq", get_req)

// COMMAND ----------

val jdbcUri = "jdbc:redshift://mikem-rs.cmyx9eo3auew.us-west-2.redshift.amazonaws.com:5439/demo"
val rsUser = "master"
val rsPass = "Master123"

val df = spark.read.format("com.databricks.spark.redshift")
  .option("url", jdbcUri)
  .option("dbtable", "mortgage_geo")
  .option("tempdir", "s3n://mikem-docs/rs-temp")
  .option("user", rsUser)
  .option("password", rsPass)
  .option("forward_spark_s3_credentials", "true")
  .load()

display(df)


// COMMAND ----------

dbutils.fs.ls("/mnt").foreach(x => print x.name)

// COMMAND ----------

val dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
val diamonds = spark.read.format("csv").option("header","true")
  .option("inferSchema", "true").load(dataPath)
diamonds.createOrReplaceTempView("diamonds")

// COMMAND ----------

dbutils.widgets.dropdown("choices", "1", Seq("1","2"))

// COMMAND ----------

// DBTITLE 1,Init Script
// MAGIC %python 
// MAGIC clusterName = "mikem-secure"
// MAGIC 
// MAGIC dbutils.fs.put("/databricks/init/mikem-secure/cluster-init.sh","""
// MAGIC #! /bin/sh
// MAGIC echo "************ in cluster-init.sh ************"
// MAGIC 
// MAGIC filename=~/.netrc
// MAGIC rm -rf $filename
// MAGIC touch $filename
// MAGIC 
// MAGIC echo 'machine 54.186.148.39\nlogin token\npassword dapi767c494109e73f6a6a21e508bc07df98' > $filename
// MAGIC 
// MAGIC sudo chmod 777 $filename
// MAGIC 
// MAGIC echo "************ Done ************"
// MAGIC """, True)
// MAGIC 
// MAGIC dbutils.fs.ls("dbfs:/databricks/init/mikem-secure/")
// MAGIC 
// MAGIC dbutils.fs.head("dbfs:/databricks/init/mikem-secure/cluster-init.sh")

// COMMAND ----------

// MAGIC %python 
// MAGIC dbutils.fs.head("dbfs:/databricks/init/mikem-secure/init.sh")

// COMMAND ----------

spark.conf.set("spark.databricks.delta.preview.enabled", "true")


import org.apache.spark.sql.functions._

// TODO use secrets
sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "get from secret")
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "get from secret")

val jdbcUri = "jdbc:redshift://mikem-rs.cmyx9eo3auew.us-west-2.redshift.amazonaws.com:5439/demo"

// Load geo data from Redshift
val df = spark.read.format("com.databricks.spark.redshift")
  .option("url", jdbcUri)
  .option("dbtable", "mortgage_geo")
  .option("tempdir", "s3n://mikem-docs")
  .option("user", "master")
  .option("password", "Master123")
  .option("forward_spark_s3_credentials", "true")
  .load.cache

df.count

//df.write.format("delta").save("/delta/geos")

//val df2 = spark.read.format("delta").load("/delta/geos")
//df2.count

val df3 = table("geos")
df3.count

// COMMAND ----------

spark.table("mortgages_with_geo")

// COMMAND ----------

// MAGIC %python 
// MAGIC ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
// MAGIC AWS_BUCKET_NAME = "mikem-docs"
// MAGIC MOUNT_NAME = "mikem-docs"
// MAGIC 
// MAGIC dbutils.fs.mount("s3a://%s" % (AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

// COMMAND ----------

// MAGIC %fs mounts