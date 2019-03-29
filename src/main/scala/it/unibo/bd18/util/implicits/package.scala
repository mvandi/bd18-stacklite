package it.unibo.bd18.util

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.reflect.ClassTag

package object implicits {

  implicit class RichSeq[T: ClassTag](private val seq: Seq[T]) {
    def toRDD(parallelism: Int)(implicit sc: SparkContext): RDD[T] = sc.makeRDD(seq, parallelism)

    def toRDD(implicit sc: SparkContext): RDD[T] = toRDD(sc.defaultParallelism)
  }

  implicit class RichSeqWithNewPartitions[T: ClassTag](private val seq: Seq[(T, Seq[String])]) {
    def toRDD(implicit sc: SparkContext): RDD[T] = sc.makeRDD(seq)
  }

  implicit class RichSparkContext(private val sc: SparkContext) {
    def executorCount: Int = sc.statusTracker.getExecutorInfos.length - 1

    def coresPerExecutor: Int = sc.range(0, 1).map(_ => Runtime.getRuntime.availableProcessors).first

    def coreCount: Int = coreCount(coresPerExecutor)

    def coreCount(coresPerExecutor: Int): Int = executorCount * coresPerExecutor
  }

  implicit class RichSparkSession(private val spark: SparkSession) {
    def readCSV(path: String, header: Boolean = true): RDD[Row] = spark
      .read
      .format("csv")
      .option("header", header.toString)
      .csv(path)
      .rdd

    def readCSVHeader(path: String): RDD[String] = spark.sparkContext.parallelize(readCSV(path, header = false)
      .take(1)
      .map(_.mkString(","))
      .toSeq)
  }

  implicit class RichSQLContext(private val sqlContext: SQLContext) {
    def apply(sqlText: String): DataFrame = sqlContext.sql(sqlText)
  }

  implicit class RichOptionRDD[T: ClassTag](private val rdd: RDD[Option[T]]) {
    def flatten: RDD[T] = rdd.filter(_.isDefined).map(_.get)
  }

  implicit class RichTraversableOnceRDD[T: ClassTag](private val rdd: RDD[TraversableOnce[T]]) {
    def flatten: RDD[T] = rdd.filter(_.nonEmpty).flatMap(identity)
  }

  implicit def comparableToOrdered[A](c: Comparable[A]): Ordered[A] = new Ordered[A] {
    override def compare(that: A): Int = c.compareTo(that)
  }

  implicit class RichDate(private val d: Date) {
    def between(start: Date, end: Date): Boolean = {
      require(start > end)
      d >= start && d <= end
    }
  }

}
