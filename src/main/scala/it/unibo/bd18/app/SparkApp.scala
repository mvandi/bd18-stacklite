package it.unibo.bd18.app

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkApp extends App {

  protected[this] val conf: SparkConf = new SparkConf()

  protected[this] final lazy val spark = SparkSession.builder.config(conf).getOrCreate()

  @inline protected[this] final def sc: SparkContext = spark.sparkContext

  protected[this] final lazy val fs = FileSystem.get(sc.hadoopConfiguration)

  object implicits {
    implicit lazy val _sc: SparkContext = sc
  }

}
