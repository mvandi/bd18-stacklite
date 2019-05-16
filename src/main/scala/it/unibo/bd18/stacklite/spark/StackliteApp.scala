package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.app.SparkApp
import it.unibo.bd18.stacklite.{Question, QuestionTag}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] trait StackliteApp extends SparkApp {

  import it.unibo.bd18.util.implicits._

  protected[this] lazy final val questionsRDD = createRDD(args(0)).map(Question.create)

  protected[this] lazy final val questionTagsRDD = createRDD(args(1)).map(QuestionTag.create)

  private[this] final def createRDD[T: ClassTag](path: String): RDD[Array[String]] = spark.readCSV(path)
    .map(_.toSeq.map(_.toString).toArray)

  protected[this] lazy final val fs = FileSystem.get(sc.hadoopConfiguration)

}
