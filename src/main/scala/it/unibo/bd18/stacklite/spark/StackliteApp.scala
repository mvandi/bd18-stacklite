package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.app.SparkApp
import it.unibo.bd18.stacklite.C.hdfs
import it.unibo.bd18.stacklite.{Question, QuestionTag}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] trait StackliteApp extends SparkApp {

  import it.unibo.bd18.util.implicits._

  protected[this] final lazy val questionsRDD = createRDD(hdfs.data.questions).map(Question.create)

  protected[this] final lazy val questionTagsRDD = createRDD(hdfs.data.questionTags).map(QuestionTag.create)

  private[this] final def createRDD[T: ClassTag](path: String): RDD[Array[String]] = spark.readCSV(path)
    .map(_.toSeq.map(_.toString).toArray)

}
