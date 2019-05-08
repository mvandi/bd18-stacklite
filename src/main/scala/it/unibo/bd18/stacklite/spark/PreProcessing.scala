package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.stacklite.Utils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf

object PreProcessing extends StackliteApp {

  import implicits._
  import it.unibo.bd18.stacklite.C.dates._
  import it.unibo.bd18.util.implicits._
  import org.apache.spark.HashPartitioner

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("PreProcessing")

  val questionsDestPath = args(2)
  val questionTagsDestPath = args(3)

  Utils.deleteIfExists(fs, true, new Path(questionsDestPath))
  Utils.deleteIfExists(fs, true, new Path(questionTagsDestPath))

  val (questions, questionTags) = questionsRDD
    .filter(_.creationDate.between(startDate, endDate))
    .keyBy(_.id)
    .join(questionTagsRDD.keyBy(_.id))
    .partitionBy(new HashPartitioner(sc.coreCount))
    .map(_._2)
    .collect
    .unzip

  spark.readCSVHeader(args(0))
    .union(questions
      .toSeq
      .toRDD
      .map(_.toCSVString))
    .saveAsTextFile(questionsDestPath)
  spark.readCSVHeader(args(1))
    .union(questionTags
      .toSeq
      .toRDD
      .map(_.toCSVString))
    .saveAsTextFile(questionTagsDestPath)

}
