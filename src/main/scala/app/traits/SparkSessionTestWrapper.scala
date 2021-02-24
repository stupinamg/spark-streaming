package app.traits

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder().
      master("local[*]")
      .appName("SparkTest")
      .getOrCreate()
  }
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SparkTest")

  val sparkContext = new SparkContext(conf)
}
