package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.stacklite.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (tags sorted in descending order).
  */
object Job1 extends StackliteApp {

  import it.unibo.bd18.util.implicits._

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job1")

  val resultPath = new Path(args(2))
  val fs = FileSystem.get(sc.hadoopConfiguration)
  if (fs.exists(resultPath)) {
    fs.delete(resultPath, true)
  }

  val outputRDD = questionsRDD.keyBy(_.id)
    .join(questionTagsRDD.keyBy(_.id))
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

  outputRDD.saveAsTextFile(args(2))

}
