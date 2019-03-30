package it.unibo.bd18.stacklite.spark

import java.util.{Calendar, Date}

import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (tags are sorted in descending order).
  */
object Job1 extends StackliteApp {

  import implicits._
  import it.unibo.bd18.util.implicits._

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job1")

  questionsRDD.keyBy(_.id)
    .partitionBy(new HashPartitioner(sc.coreCount))
    .join(questionTagsRDD.keyBy(_.id))
    .mapPair((_, x) => (getYearMonthPair(x._1.creationDate), (x._2.tag, x._1.score)))
    .groupByKey
    .mapValues(_.toRDD
      .groupByKey
      .mapValues(_.sum)
      .sortBy(-_._2)
      .map(_._1)
      .take(5)
      .mkString("[", ", ", "]"))
    .mapPair((x, y) => s"$x -> $y")
    .saveAsTextFile(args(2))

  private def getYearMonthPair(d: Date): (Int, Int) = {
    val c = Calendar.getInstance()
    c.setTime(d)
    (c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1)
  }

}
