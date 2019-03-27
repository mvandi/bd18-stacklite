package it.unibo.bd18.app

import org.apache.spark.{SparkConf, SparkContext}

private[app] trait SparkAppBase extends App {

  protected[this] val conf: SparkConf = new SparkConf()

  protected[this] def sc: SparkContext

  object implicits {
    println(s"$getClass Implicits object created.")
    implicit lazy val _sc: SparkContext = sc
  }

}
