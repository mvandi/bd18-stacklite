package it.unibo.bd18.stacklite

import it.unibo.bd18.app.SQLApp
import it.unibo.bd18.util._
import org.apache.spark.{HashPartitioner, SparkConf}

object PreProcessing extends SQLApp {

  import SQLImplicits._
  import implicits._
  import it.unibo.bd18.util.implicits._
  import QuestionData.df

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("PreProcessing")

  val questionsSrcPath: String = args(0)
  val questionsDestPath: String = args(1)

  val questionTagsSrcPath: String = args(2)
  val questionTagsDestPath: String = args(3)

  val questionsRDD = loadCSV(questionsSrcPath).map(QuestionData.extract)
  val questionTagsRDD = loadCSV(questionTagsSrcPath).map(QuestionTagData.extract)

  val startDate = sc.broadcast(df.parse("2012-01-01T00:00:00Z"))
  val endDate = sc.broadcast(df.parse("2017-01-01T00:00:00Z"))

  val (questions, questionTags) = questionsRDD
    .filter(x => x.creationDate.between(startDate.value, endDate.value))
    .keyBy(_.id)
    .partitionBy(new HashPartitioner(4 * sc.coreCount))
    .join(questionTagsRDD.keyBy(_.id))
    .map(_._2)
    .collect
    .unzip

  loadCSVHeader(questionsSrcPath)
    .union(questions
      .toSeq
      .toRDD
      .map(_.toCSVString))
    .saveAsTextFile(questionsDestPath)
  loadCSVHeader(questionTagsSrcPath)
    .union(questionTags
      .toSeq
      .toRDD
      .map(_.toCSVString))
    .saveAsTextFile(questionTagsDestPath)

}
