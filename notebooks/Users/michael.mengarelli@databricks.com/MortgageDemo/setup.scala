// Databricks notebook source
import org.apache.spark.sql.functions._

val jdbcUri = "jdbc:redshift://mikem-redshift-demo.cmyx9eo3auew.us-west-2.redshift.amazonaws.com:5439/demo"

val rsUser = dbutils.secrets.get(scope = "mikem", key = "rs_username")
val rsPass = dbutils.secrets.get(scope = "mikem", key = "rs_password")