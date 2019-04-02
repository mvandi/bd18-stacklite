package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.app.SparkApp
import it.unibo.bd18.stacklite.{QuestionData, QuestionTagData}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] trait StackliteApp extends SparkApp {

  import it.unibo.bd18.util.implicits._

  protected[this] lazy final val questionsRDD = createRDD(args(0))(QuestionData(_))

  protected[this] lazy final val questionTagsRDD = createRDD(args(1))(QuestionTagData(_))

  private def createRDD[T: ClassTag](file: String)(f: Array[String] => T): RDD[T] = spark.readCSV(file)
    .map(_.toSeq.map(_.toString).toArray)
    .map(f)

}
