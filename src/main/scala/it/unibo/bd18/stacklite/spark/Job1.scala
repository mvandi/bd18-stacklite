package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.stacklite.Utils
import it.unibo.bd18.util.implicits._
import org.apache.hadoop.fs.Path

/**
  * Find the first five tags that received the highest sum of scores for each
  * year-month pair (tags sorted in descending order).
  */
object Job1 extends StackliteApp {

  import it.unibo.bd18.stacklite.C.{dates, tuning}
  import org.apache.spark.{HashPartitioner, SparkConf}

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Main")

  val resultPath = args(2)
  Utils.deleteIfExists(fs, true, new Path(resultPath))

  val outputRDD = {
    val questionsRDD = this.questionsRDD
      .filter(_.creationDate.between(dates.startDate, dates.endDate))
      .keyBy(_.id)

    val questionTagsRDD = this.questionTagsRDD.keyBy(_.id)

    questionsRDD.join(questionTagsRDD)
      .mapPair((_, x) => (Utils.format(x._1.creationDate), (x._2.tag, x._1.score)))
      .groupByKey
      .partitionBy(new HashPartitioner(tuning.cpu.executorCount * 4))
      .mapValues(_.groupByKey
        .mapValues(_.sum)
        .toSeq
        .sortBy(-_._2)
        .take(5)
//        .map(_._1)
        .mkString("[", ", ", "]"))
      .mapPair((x, y) => s"$x\t$y")
  }

  println(s"\n${outputRDD.toDebugString}\n")

  outputRDD.saveAsTextFile(resultPath)

}
