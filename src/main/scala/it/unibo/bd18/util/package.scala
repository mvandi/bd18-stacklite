package it.unibo.bd18

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import util.implicits.RichSeq

package object util {

  def loadCSV(path: String, header: Boolean = true)(implicit spark: SparkSession, sc: SparkContext): RDD[Row] = spark
    .read
    .format("csv")
    .option("header", header.toString)
    .csv(path)
    .rdd

  def loadCSVHeader(path: String)(implicit spark: SparkSession, sc: SparkContext): RDD[String] = loadCSV(path, header = false)
    .take(1)
    .toSeq
    .toRDD
    .map(_.mkString(","))

}
