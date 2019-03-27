package it.unibo.bd18.app

import org.apache.spark.streaming.{Duration, StreamingContext}

trait StreamingApp extends SparkApp {

  protected[this] val batchDuration: Duration

  protected[this] lazy val ssc = new StreamingContext(sc, batchDuration)

  object streamingImplicits {
    implicit val _ssc: StreamingContext = ssc
  }

}
