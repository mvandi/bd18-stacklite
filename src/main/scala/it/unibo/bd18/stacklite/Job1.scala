package it.unibo.bd18.stacklite

import org.apache.spark.SparkConf

object Job1 extends StackliteApp {

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job1")

}
