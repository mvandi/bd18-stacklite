package it.unibo.bd18.stacklite

sealed trait QuestionTagData {

  def id: Int

  def tag: String

  def toCSVString: String = s"$id,$tag"

}

object QuestionTagData {

  def apply(row: Array[String]): QuestionTagData = QuestionTagDataImpl(
    getId(row),
    getTag(row))

  private def getId(row: Array[String]): Int = row(0).toInt

  private def getTag(row: Array[String]): String = row(1)

  private case class QuestionTagDataImpl(
                                          override val id: Int,
                                          override val tag: String
                                        ) extends QuestionTagData

}
