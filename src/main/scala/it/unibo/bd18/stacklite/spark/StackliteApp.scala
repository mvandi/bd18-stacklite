package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.app.SparkApp
import it.unibo.bd18.stacklite.C.{hdfs, parquet}
import it.unibo.bd18.stacklite.{Question, QuestionTag, Utils}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, types}

import scala.reflect.ClassTag

private[spark] trait StackliteApp extends SparkApp {

  import it.unibo.bd18.util.implicits._

  protected[this] lazy final val questionsRDD = createRDD(hdfs.data.questions).map(Question.create)

  protected[this] lazy final val questionTagsRDD = createRDD(hdfs.data.questionTags).map(QuestionTag.create)

  private[this] final def createRDD[T: ClassTag](path: String): RDD[Array[String]] = spark.readCSV(path)
    .map(_.toSeq.map(_.toString).toArray)

  protected[this] lazy final val fs = FileSystem.get(sc.hadoopConfiguration)

}
