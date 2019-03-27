package it.unibo.bd18.app

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

trait SQLApp extends SparkAppBase {

  protected[this] final lazy val spark = SparkSession.builder.config(conf).getOrCreate()

  protected[this] override final def sc: SparkContext = spark.sparkContext

  protected[this] final def sql: SQLContext = spark.sqlContext

  protected[this] final def sql(sqlText: String): DataFrame = spark.sql(sqlText)

  object SQLImplicits {
    implicit val _spark: SparkSession = spark
    implicit val _sql: SQLContext = sql
  }

}
