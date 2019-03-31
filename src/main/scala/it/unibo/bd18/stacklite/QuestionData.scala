package it.unibo.bd18.stacklite

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import it.unibo.bd18.stacklite.QuestionData.df

sealed trait QuestionData {

  def id(): Int

  def creationDate(): Date

  def closedDate(): Option[Date]

  def deletionDate(): Option[Date]

  def score(): Int

  def ownerUserId(): Option[Int]

  def answerCount(): Option[Int]

  def toCSVString(): String = s"$id,${df.format(creationDate)},${toString(closedDate)(df.format)},${toString(deletionDate)(df.format)},$score,${toString(ownerUserId)(_.toString)},${toString(answerCount)(_.toString)}"

  private def toString[T](opt: Option[T])(f: T => String): String = opt.map(f).getOrElse("NA")

}

object QuestionData {

  def apply(row: Array[String]): QuestionData = create(row)

  def create(row: Array[String]): QuestionData = QuestionDataImpl(
    getId(row),
    getCreationDate(row),
    getClosedDate(row),
    getDeletionDate(row),
    getScore(row),
    getOwnerUserId(row),
    getAnswerCount(row))

  private[stacklite] val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

  private def getId(row: Array[String]): Int = row(0).toInt

  private def getCreationDate(row: Array[String]): Date = df.parse(row(1))

  private def getClosedDate(row: Array[String]): Option[Date] = getOption(row)(_(2))(_ != "NA").map(df.parse)

  private def getDeletionDate(row: Array[String]): Option[Date] = getOption(row)(_(3))(_ != "NA").map(df.parse)

  private def getScore(row: Array[String]): Int = row(4).toInt

  private def getOwnerUserId(row: Array[String]): Option[Int] = getOption(row)(_(5))(_ != "NA").map(_.toInt)

  private def getAnswerCount(row: Array[String]): Option[Int] = getOption(row)(_(6))(_ != "NA").map(_.toInt)

  private def getOption[T](row: Array[String])(f: Array[String] => T)(g: T => Boolean): Option[T] = Some(row).map(f).filter(g)

  private case class QuestionDataImpl(
                                       override val id: Int,
                                       override val creationDate: Date,
                                       override val closedDate: Option[Date],
                                       override val deletionDate: Option[Date],
                                       override val score: Int,
                                       override val ownerUserId: Option[Int],
                                       override val answerCount: Option[Int]
                                     ) extends QuestionData

}
