package com.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf


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

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    val df = Seq(
      (1, "fn", "red"),
      (2, "fn", "blue"),
      (3, "fn", "green"),
      (4, "aa", "blue"),
      (5, "aa", "green"),
      (6, "bb", "red"),
      (7, "bb", "red"),
      (8, "aa", "blue")
    ).toDF("id", "dept", "color")

    df.createOrReplaceTempView("main_table")

    val replace_val = udf((x: String,y:String) => if (Option(x).getOrElse("").equalsIgnoreCase("fn")&&(!y.equalsIgnoreCase("red"))) "red" else y)

    sparkSession.sql("select * from main_table").show()
    sparkSession.sql("create table temp_table like main_table")
    sparkSession.sql(s"insert into table temp_table (id,dept,color) select id,dept,$replace_val(dept,color) from main_table")

    /*sparkSession.sql(s"insert into table temp_table (id,dept,color) select id,dept,$replace_val(dept,color) from main_table").show()*/

    //val replace_val = udf((x: String,y:String) => if (Option(x).getOrElse("").equalsIgnoreCase("fn")&&(!y.equalsIgnoreCase("red"))) "red" else y)
    /*val final_df = df.withColumn("color", replace_val($"dept",$"color"))
    final_df.show()*/

    println("in main")
    sparkSession.close
  }
}
