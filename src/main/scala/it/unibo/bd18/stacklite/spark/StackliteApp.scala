package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.app.SparkApp
import it.unibo.bd18.stacklite.{QuestionData, QuestionTagData}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] trait StackliteApp extends SparkApp {

  import it.unibo.bd18.util.implicits._

  protected[this] lazy final val questionsRDD = createRDD(args(0))(QuestionData.create)

  protected[this] lazy final val questionTagsRDD = createRDD(args(1))(QuestionTagData.create)

  private def createRDD[T: ClassTag](path: String)(f: Array[String] => T): RDD[T] = spark.readCSV(path)
    .map(_.toSeq.map(_.toString).toArray)
    .map(f)

  protected[this] def deleteIfExists(path: String): Unit = Some(path)
    .map(new Path(_))
    .filter(fs.exists)
    .foreach(fs.delete(_, true))

  private lazy val fs = FileSystem.get(sc.hadoopConfiguration)

}
