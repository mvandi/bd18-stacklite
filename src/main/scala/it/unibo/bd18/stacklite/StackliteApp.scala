package it.unibo.bd18.stacklite

import java.text.{DateFormat, SimpleDateFormat}

import it.unibo.bd18.app.SQLApp
import it.unibo.bd18.util._

private[stacklite] trait StackliteApp extends SQLApp {

  implicit val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  import implicits._
  import SQLImplicits._

  protected[this] lazy final val questionsRDD = loadCSV(args(0)).map(QuestionData.extract).cache()
  protected[this] lazy final val questionTagsRDD = loadCSV(args(1)).map(QuestionTagData.extract).cache()

}
