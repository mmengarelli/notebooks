# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS books
# MAGIC USING xml
# MAGIC OPTIONS (path "/mnt/mikem/data/books.xml", rowTag "book");
# MAGIC 
# MAGIC select * from books

# COMMAND ----------

df = spark.read.format("xml").option("rowTag", "book").load("/mnt/mikem/data/books.xml")
display(df)

# COMMAND ----------

# DBTITLE 1,UnsupportedOperationException
# df = spark.readStream.format("xml").option("rowTag", "book").load("/mnt/mikem/data/books.xml")
# display(df)

# COMMAND ----------

# MAGIC %fs head /mnt/mikem/data/books.xml

# COMMAND ----------

# DBTITLE 1,Read XML from Binary File into String Col
# MAGIC %scala
# MAGIC import com.databricks.spark.xml.functions.from_xml
# MAGIC import com.databricks.spark.xml.schema_of_xml
# MAGIC 
# MAGIC val toStrUDF = udf((bytes: Array[Byte]) => new String(bytes, "UTF-8"))
# MAGIC 
# MAGIC val df = spark.read.format("binaryFile")
# MAGIC   .load("/mnt/mikem/data/books.xml")
# MAGIC   .select(toStrUDF($"content").alias("text"))
# MAGIC 
# MAGIC val payloadSchema = schema_of_xml(df.select("text").as[String])
# MAGIC 
# MAGIC val df_mod = df.select(from_xml($"text", payloadSchema).alias("parsed"))
# MAGIC 
# MAGIC df_mod.printSchema

# COMMAND ----------

# MAGIC %scala 
# MAGIC display(df_mod.select("parsed.book.author"))

# COMMAND ----------

# MAGIC %md # Scala

# COMMAND ----------

# MAGIC %scala val file = "/dbfs/mnt/mikem/data/books.xml"

# COMMAND ----------

# MAGIC %md ## SAX Parser
# MAGIC 
# MAGIC Part of JDK, no need to install libraries
# MAGIC 
# MAGIC The Simple API for XML (SAX) is a push API; this SAX parser sends (push) the XML data to the client continuously. In SAX, the client has no control of when to receive the XML data.
# MAGIC 
# MAGIC [Example](https://mkyong.com/java/how-to-read-xml-file-in-java-sax-parser/)

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Date
# MAGIC import java.util.ArrayList
# MAGIC 
# MAGIC class Book() {
# MAGIC //Book(id:String, author:String, description:String, 
# MAGIC //           genre:String, price:Double, publishDate:Date, title:String) {
# MAGIC   
# MAGIC   private var _id:String = _
# MAGIC   private var _author:String = _
# MAGIC   private var _description:String = _
# MAGIC   private var _genre:String = _
# MAGIC   private var _price:Double = _
# MAGIC   private var _publishDate:Date = _
# MAGIC   private var _title:String = _
# MAGIC   
# MAGIC   def id = _id
# MAGIC   def id_=(id: String) {
# MAGIC     _id = id
# MAGIC   }
# MAGIC   
# MAGIC   def author = _author
# MAGIC   def author_=(author: String) {
# MAGIC     _author = author
# MAGIC   }
# MAGIC 
# MAGIC //   def description = _description
# MAGIC //   def description_ =(description: String) {
# MAGIC //     _description = description
# MAGIC //   }
# MAGIC   
# MAGIC   def genre = _genre
# MAGIC   def genre_=(genre: String) {
# MAGIC     _genre = genre
# MAGIC   }
# MAGIC 
# MAGIC   def price = _price
# MAGIC   def price_=(price: Double) {
# MAGIC     _price = price
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def displayBook(book:Book) = {
# MAGIC     println("Author" + book.author)
# MAGIC     println("Price" + book.price)
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.xml.sax.helpers.DefaultHandler
# MAGIC import org.xml.sax.Attributes
# MAGIC 
# MAGIC class SimpleHandler extends DefaultHandler {
# MAGIC   var books:ArrayList[Book] = null
# MAGIC   var book:Book = _
# MAGIC   var currentValue:StringBuilder = new StringBuilder();
# MAGIC 
# MAGIC   def getResult:ArrayList[Book] = {
# MAGIC     books
# MAGIC   }
# MAGIC   
# MAGIC   override def startDocument() = {
# MAGIC   }
# MAGIC   
# MAGIC   override def endDocument() = {
# MAGIC   }
# MAGIC 
# MAGIC   override def startElement(uri:String, localName:String, qName:String, attributes:Attributes) = {
# MAGIC     qName match {
# MAGIC       case "catalog" => {
# MAGIC         books = new ArrayList[Book]()
# MAGIC       }
# MAGIC       case "book" => {
# MAGIC         book = new Book()
# MAGIC         books.add(book)
# MAGIC       }
# MAGIC //       case "author" => book.author = attributes.getValue(qName)
# MAGIC // //       case "title" => book.title = attributes.getValue(qName)
# MAGIC //        case "genre" => book.genre = attributes.getValue(qName)
# MAGIC //        case "price" => book.price = attributes.getValue(qName).toDouble
# MAGIC // //       case "publish_date" => book.publishDate = attributes.getValue(qName)
# MAGIC // //      case "description" => book.description = attributes.getValue(qName)
# MAGIC       case _ => {
# MAGIC         //println("Other")
# MAGIC       }
# MAGIC     }
# MAGIC   }
# MAGIC 
# MAGIC   override def endElement(uri:String, localName:String, qName:String) = {
# MAGIC     qName match {
# MAGIC       case "catalog" => {
# MAGIC       }
# MAGIC       case "book" => {
# MAGIC       }
# MAGIC       case "author" => {
# MAGIC         book.author = currentValue.toString
# MAGIC       }
# MAGIC       case "title" => {
# MAGIC         //book.title
# MAGIC       }
# MAGIC       case "genre" => {
# MAGIC         book.genre = currentValue.toString
# MAGIC       }
# MAGIC       case "price" => {
# MAGIC         book.price=123
# MAGIC       }
# MAGIC       case "publish_date" => {
# MAGIC       }
# MAGIC       case "description" => {
# MAGIC       }
# MAGIC       case _ => {
# MAGIC       }
# MAGIC   }
# MAGIC  }
# MAGIC 
# MAGIC   override def characters(ch:Array[Char], start:Int, length:Int) = {
# MAGIC     currentValue.append(ch.mkString)
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import javax.xml.parsers.SAXParser;
# MAGIC import javax.xml.parsers.SAXParserFactory;
# MAGIC import org.xml.sax.XMLReader;
# MAGIC 
# MAGIC val factory:SAXParserFactory = SAXParserFactory.newInstance();
# MAGIC val parser:SAXParser = factory.newSAXParser();
# MAGIC val xmlReader:XMLReader = parser.getXMLReader();
# MAGIC val handler:SimpleHandler = new SimpleHandler() 
# MAGIC 
# MAGIC val result = parser.parse(file, handler)
# MAGIC 
# MAGIC val catalog:ArrayList[Book] = handler.getResult
# MAGIC println(catalog.size)
# MAGIC catalog.forEach{displayBook}

# COMMAND ----------

# MAGIC %md ## STaX Parser
# MAGIC 
# MAGIC The Streaming API for XML (StAX) is a pull API; the client calls methods on the StAX parser library to get (pull) the XML data one by one manually. In StAX, the client in control of when to get (pull) the XML data.
# MAGIC 
# MAGIC [Example](https://mkyong.com/java/how-to-read-xml-file-in-java-stax-parser/)

# COMMAND ----------

# MAGIC %scala
# MAGIC import javax.xml.stream.{XMLInputFactory, XMLStreamReader}
# MAGIC import java.io.FileInputStream
# MAGIC import javax.xml.stream.XMLStreamConstants._
# MAGIC 
# MAGIC val factory:XMLInputFactory = XMLInputFactory.newInstance()
# MAGIC val reader:XMLStreamReader = factory.createXMLStreamReader(new FileInputStream(file))
# MAGIC var eventType = reader.getEventType()
# MAGIC 
# MAGIC while (reader.hasNext()) {
# MAGIC   eventType = reader.next()
# MAGIC 
# MAGIC   eventType match {
# MAGIC     case START_ELEMENT => {startElement(reader.getName().getLocalPart())}
# MAGIC     case END_ELEMENT => {endElement(reader.getName().getLocalPart())}
# MAGIC     case _ => {"other event"}
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def startElement(name:String) = {
# MAGIC   name match {
# MAGIC     case "catalog" => {println("catalog")}
# MAGIC     case "book" => {println("book")}
# MAGIC     case "author" => {println("author")}
# MAGIC     case "title" => {println("title")}
# MAGIC     case "genre" => {println("genre")}
# MAGIC     case "description" => {println("description")}
# MAGIC     case "price" => {println("price")}
# MAGIC     case "publish_date" => {println("publish_date")}
# MAGIC     case _ => {println("other")}
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC def endElement(name:String) = {
# MAGIC   println(s"end: $name")
# MAGIC }

# COMMAND ----------

# MAGIC %md # Python

# COMMAND ----------

# MAGIC %md ## XML Stream

# COMMAND ----------

# MAGIC %pip install xml_stream

# COMMAND ----------

file = "/dbfs/mnt/mikem/data/books.xml"

# COMMAND ----------

from xml_stream import read_xml_file

for root in read_xml_file(file, records_tag='catalog'):
  for el in root.iter():
    tag = el.tag

    if(tag == "catalog"):
      books = []
    elif(tag == "book"):
      book = {}
      books.append(book)
    else:
      book[tag] = el.text
    
books

# COMMAND ----------

# MAGIC %md ## SAX

# COMMAND ----------

from xml.sax import ContentHandler

class SimpleHandler(ContentHandler):
  def __init__(self):
    self.books = []
    self.book = {}

  def startElement(self, tag, attributes):
    print("start element", tag)

  def endElement(self, tag):
    if(tag == "catalog"):
      self.books = []
    elif(tag == "book"):
      self.books.append(book)
    else:
      book[tag] = el.text

  def characters(self, content):
    self.book[tag] = content

# COMMAND ----------

import xml.sax as sax

handler = SimpleHandler()

parser = sax.make_parser()
parser.setContentHandler(handler)
result = parser.parse(file)
print(result)

# COMMAND ----------

# MAGIC %md ## xml.dom.pulldom 

# COMMAND ----------

from xml.dom.pulldom import parse
from xml.sax import make_parser
from xml.sax.handler import feature_external_ges

parser = make_parser()
parser.setFeature(feature_external_ges, True)
parse(file, parser=parser)

# COMMAND ----------

from xml.dom import pulldom

doc = pulldom.parse(file)
for event, node in doc:
    if event == pulldom.START_ELEMENT and node.tagName == 'item':
        if int(node.getAttribute('price')) > 50:
            doc.expandNode(node)
            print(node.toxml())
