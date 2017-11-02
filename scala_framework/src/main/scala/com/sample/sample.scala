package com.sample

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

object sample {
  val logger = LoggerFactory.getLogger(sample.getClass)
  def getSparkConf(): SparkConf = {
    new SparkConf()
  }
  def getSparkSession(sparkConf: SparkConf): SparkSession = {
    val builder = SparkSession.builder
    builder.appName("test")
    builder.master("local")
    val sparkSession = builder.getOrCreate()
    sparkSession
  }
  def getContext(sparkSession: SparkSession): (SparkContext, SQLContext) = {
    (sparkSession.sparkContext, sparkSession.sqlContext)
  }
  def main(args: Array[String]): Unit = {
    val conf = getSparkConf
    val sparkSession = getSparkSession(conf)

    import sparkSession.implicits._
    //sparkSession.read.textFile("fileLocation").as[String]
    //sparkSession.read.csv("fileLocation")
    //sparkSession.read.format("com.databricks.spark.avro").load("fileLocation")

    println("in main")
    sparkSession.close
  }
}