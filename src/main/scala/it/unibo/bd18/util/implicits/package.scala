package it.unibo.bd18.util

import java.util.Date

import it.unibo.bd18.stacklite.Utils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

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

  implicit class RichSparkSession(private val spark: SparkSession) {
    def readCSV(path: String, header: Boolean = true): RDD[Row] = spark.read
      .format("csv")
      .option("header", header.toString)
      .csv(path)
      .rdd
  }

  implicit class RichDate(private val d: Date) {
    def between(start: Date, end: Date): Boolean = Utils.between(d, start, end)
  }

}
