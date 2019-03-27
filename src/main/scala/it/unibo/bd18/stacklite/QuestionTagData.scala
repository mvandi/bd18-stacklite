package it.unibo.bd18.stacklite

import org.apache.spark.sql.Row

trait QuestionTagData {

  def id(): Int
  def tag(): String

  def toCSVString(): String = s"$id,$tag"

}

object QuestionTagData {

  def extract(row: Row): QuestionTagData = QuestionTagDataImpl(
    getId(row),
    getTag(row))

  private def getId(row: Row): Int = row.getInt(0)

  private def getTag(row: Row): String = row.getString(1)

  private case class QuestionTagDataImpl(
                                       override val id: Int,
                                       override val tag: String
                                     ) extends QuestionTagData

}
