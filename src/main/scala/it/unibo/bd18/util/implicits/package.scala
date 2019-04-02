package it.unibo.bd18.util

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.language.implicitConversions
import scala.reflect.ClassTag

package object implicits {

  implicit class RichTraversableOnce[T: ClassTag](private val t: TraversableOnce[T]) {
    def toRDD(parallelism: Int)(implicit sc: SparkContext): RDD[T] = sc.makeRDD(t.toSeq, parallelism)

    def toRDD(implicit sc: SparkContext): RDD[T] = toRDD(sc.defaultParallelism)
  }

  implicit class RichTraversableOnceWithNewPartitions[T: ClassTag](private val t: TraversableOnce[(T, TraversableOnce[String])]) {
    def toRDD(implicit sc: SparkContext): RDD[T] = sc.makeRDD(t.map(x => (x._1, x._2.toSeq)).toSeq)
  }

  implicit class RichKeyValueTraversable[K, V](private val t: Traversable[(K, V)]) {
    def groupByKey: Map[K, Traversable[V]] = t.groupBy(_._1).mapValues(_.map(_._2))
  }

  implicit class RichPairRDD[K, V](private val rdd: RDD[(K, V)]) {
    def filterPair(f: (K, V) => Boolean): RDD[(K, V)] = rdd.filter(x => f(x._1, x._2))

    def flatMapPair[U: ClassTag](f: (K, V) => TraversableOnce[U]): RDD[U] = rdd.flatMap(x => f(x._1, x._2))

    def mapPair[U: ClassTag](f: (K, V) => U): RDD[U] = rdd.map(x => f(x._1, x._2))
  }

  implicit class RichOptionRDD[T: ClassTag](private val rdd: RDD[Option[T]]) {
    def flatten: RDD[T] = rdd.filter(_.isDefined).map(_.get)
  }

  implicit class RichTraversableOnceRDD[T: ClassTag](private val rdd: RDD[TraversableOnce[T]]) {
    def flatten: RDD[T] = rdd.filter(_.nonEmpty).flatMap(identity)
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

    def readCSVHeader(path: String): RDD[String] = readCSV(path, header = false)
      .take(1)
      .map(_.mkString(","))
      .toSeq
      .toRDD(spark.sparkContext)
  }

  implicit class RichSQLContext(private val sqlContext: SQLContext) {
    def apply(sqlText: String): DataFrame = sqlContext.sql(sqlText)
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
