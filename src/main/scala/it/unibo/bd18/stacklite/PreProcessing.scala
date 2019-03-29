package it.unibo.bd18.stacklite

import it.unibo.bd18.app.SparkApp
import org.apache.spark.{HashPartitioner, SparkConf}

object PreProcessing extends SparkApp {

  import QuestionData.df
  import implicits._
  import it.unibo.bd18.util.implicits._

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("PreProcessing")

  val questionsSrcPath = args(0)
  val questionsDestPath = args(1)
  val questionTagsSrcPath = args(2)
  val questionTagsDestPath = args(3)

  val questionsRDD = spark.readCSV(questionsSrcPath).map(QuestionData.extract)
  val questionTagsRDD = spark.readCSV(questionTagsSrcPath).map(QuestionTagData.extract)

  val startDate = df.parse("2011-12-31T23:59:59Z")
  val endDate = df.parse("2017-01-01T00:00:00Z")

  val (questions, questionTags) = questionsRDD
    .filter(_.creationDate.between(startDate, endDate))
    .keyBy(_.id)
    .partitionBy(new HashPartitioner(sc.coreCount))
    .join(questionTagsRDD.keyBy(_.id))
    .map(_._2)
    .collect
    .unzip

  spark.readCSVHeader(questionsSrcPath)
    .union(questions
      .toSeq
      .toRDD
      .map(_.toCSVString))
    .saveAsTextFile(questionsDestPath)
  spark.readCSVHeader(questionTagsSrcPath)
    .union(questionTags
      .toSeq
      .toRDD
      .map(_.toCSVString))
    .saveAsTextFile(questionTagsDestPath)

}
