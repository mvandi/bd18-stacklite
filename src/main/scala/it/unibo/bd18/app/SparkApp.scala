package it.unibo.bd18.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkApp extends App {

  protected[this] val conf: SparkConf = new SparkConf()

  protected[this] final lazy val spark = SparkSession.builder.config(conf).getOrCreate()

  @inline protected[this] final def sc: SparkContext = spark.sparkContext

  object implicits {
    implicit lazy val _sc: SparkContext = sc
  }

}
