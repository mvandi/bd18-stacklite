package it.unibo.bd18.util

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

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

    def coresPerExecutor: Int = sc.range(0, 1).map(_ => Runtime.getRuntime.availableProcessors).collect.head

    def coreCount: Int = coreCount(coresPerExecutor)

    def coreCount(coresPerExecutor: Int): Int = executorCount * coresPerExecutor
  }

  implicit class RichSQLContext(private val sqlContext: SQLContext) {
    def apply(sqlText: String): DataFrame = sqlContext.sql(sqlText)
  }

  implicit class RichOptionRDD[T: ClassTag](private val rdd: RDD[Option[T]]) {
    def flatten: RDD[T] = rdd.filter(_.isDefined).map(_.get)
  }

  implicit class RichPairRDD[K, V](private val rdd: RDD[(K, V)]) {
    def filterPair(f: (K, V) => Boolean): RDD[(K, V)] = rdd.filter(x => f(x._1, x._2))

    def filterByKey(f: K => Boolean): RDD[(K, V)] = rdd.filter(t => f(t._1))

    def filterByValue(f: V => Boolean): RDD[(K, V)] = rdd.filter(t => f(t._2))

    def flatMapPair[U: ClassTag](f: (K, V) => TraversableOnce[U]): RDD[U] = rdd.flatMap(x => f(x._1, x._2))

    def flatMapKeys[U](f: K => TraversableOnce[U]): RDD[(U, V)] = rdd.flatMap(x => f(x._1).map((_, x._2)))

    def mapPair[U: ClassTag](f: (K, V) => U): RDD[U] = rdd.map(x => f(x._1, x._2))

    def mapKeys[U](f: K => U): RDD[(U, V)] = rdd.map(x => (f(x._1), x._2))
  }

  implicit class RichKeyOptionRDD[K: ClassTag, V: ClassTag](private val rdd: RDD[(Option[K], V)]) {
    def flatten: RDD[(K, V)] = rdd.filterByKey(_.isDefined).mapKeys(_.get)
  }

  implicit class RichValueOptionRDD[K: ClassTag, V: ClassTag](private val rdd: RDD[(K, Option[V])]) {
    def flatten: RDD[(K, V)] = rdd.filterByValue(_.isDefined).mapValues(_.get)
  }

  implicit class RichTraversableOnceKeyRDD[K: ClassTag, V: ClassTag](private val rdd: RDD[(TraversableOnce[K], V)]) {
    def flatten: RDD[(K, V)] = rdd.filterByKey(_.nonEmpty).flatMapKeys(identity)
  }

  implicit class RichTraversableOnceValueRDD[K: ClassTag, V: ClassTag](private val rdd: RDD[(K, TraversableOnce[V])]) {
    def flatten: RDD[(K, V)] = rdd.filterByValue(_.nonEmpty).flatMapValues(identity)
  }

  implicit class RichDate(private val d: Date) {
    def between(start: Date, end: Date): Boolean = {
      require(start.before(end))
      !(d.before(start) | end.after(end))
    }
  }

}
