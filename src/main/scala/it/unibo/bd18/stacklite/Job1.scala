package it.unibo.bd18.stacklite

import java.util.{Calendar, Date}

import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (sorted in descending order);
  */
object Job1 extends StackliteApp {

  import implicits._
  import it.unibo.bd18.util.implicits._

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job1")

  val result = questionsRDD.keyBy(_.id)
    .partitionBy(new HashPartitioner(sc.coreCount))
    .join(questionTagsRDD.keyBy(_.id))
    .map(x => (getYearMonthPair(x._2._1.creationDate), (x._2._2.tag, x._2._1.score)))
    .groupByKey()
    .mapValues(_.toRDD
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
      .sortBy(-_._2)
      .map(_._1)
      .take(5)
      .toList)

  private def getYearMonthPair(d: Date): (Int, Int) = {
    val c = Calendar.getInstance()
    c.setTime(d)
    (c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1)
  }

}
