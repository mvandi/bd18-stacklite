package it.unibo.bd18.stacklite.spark

import org.apache.spark.SparkConf

object Job2 extends StackliteSQLApp {

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job2")

}
