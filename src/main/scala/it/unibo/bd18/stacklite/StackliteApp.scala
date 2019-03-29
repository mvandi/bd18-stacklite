package it.unibo.bd18.stacklite

import it.unibo.bd18.app.SparkApp

private[stacklite] trait StackliteApp extends SparkApp {

  import it.unibo.bd18.util.implicits._

  protected[this] lazy final val questionsRDD = spark.readCSV(args(0)).map(QuestionData.extract).cache()
  protected[this] lazy final val questionTagsRDD = spark.readCSV(args(1)).map(QuestionTagData.extract).cache()

}
