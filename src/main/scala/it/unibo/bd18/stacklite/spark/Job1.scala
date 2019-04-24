package it.unibo.bd18.stacklite.spark

import org.apache.spark.SparkConf

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (tags sorted in descending order).
  */
object Job1 extends StackliteApp {

  import it.unibo.bd18.stacklite.C.dates._
  import it.unibo.bd18.stacklite.Utils
  import it.unibo.bd18.util.implicits._
  import org.apache.spark.HashPartitioner

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job1")

  val resultPath = args(2)
  deleteIfExists(resultPath)

  val qRDD = questionsRDD
    .filter(_.creationDate.between(startDate, endDate))
    .keyBy(_.id)

  val qtRDD = questionTagsRDD.keyBy(_.id)

  val outputRDD = qRDD
    .join(qtRDD)
    .mapPair((_, x) => (Utils.format(x._1.creationDate), (x._2.tag, x._1.score)))
    .groupByKey
    .partitionBy(new HashPartitioner(sc.coreCount))
    .mapValues(_.groupByKey
      .mapValues(_.sum)
      .toStream
      .sortBy(-_._2)
      .take(5)
      .mkString("[", ", ", "]"))
    .mapPair((x, y) => s"$x -> $y")

  println(s"\n${outputRDD.toDebugString}\n")

  outputRDD.saveAsTextFile(resultPath)

}
