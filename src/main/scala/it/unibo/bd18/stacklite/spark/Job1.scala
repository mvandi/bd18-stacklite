package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.stacklite.YearMonthPair
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (tags are sorted in descending order).
  */
object Job1 extends StackliteApp {

  import implicits._
  import it.unibo.bd18.util.implicits._

  override protected[this] val conf = new SparkConf().setAppName("Job1")

  val resultPath = new Path(args(2))
  val fs = FileSystem.get(sc.hadoopConfiguration)
  if (fs.exists(resultPath)) {
    fs.delete(resultPath, true)
  }

  questionsRDD.keyBy(_.id)
    .partitionBy(new HashPartitioner(sc.coreCount))
    .join(questionTagsRDD.keyBy(_.id))
    .mapPair((_, x) => (YearMonthPair(x._1.creationDate), (x._2.tag, x._1.score)))
    .groupByKey
    .mapValues(_.toRDD
      .groupByKey
      .mapValues(_.sum)
      .sortBy(_._2, ascending = false)
      .map(_._1)
      .take(5)
      .mkString("[", ", ", "]"))
    .mapPair((x, y) => s"$x -> $y")
    .saveAsTextFile(args(2))

}
