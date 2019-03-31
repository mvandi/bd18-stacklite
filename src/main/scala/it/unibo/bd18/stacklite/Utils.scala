package it.unibo.bd18.stacklite

object Utils {

  private val headerRegex = "(^Id,\\s*|,\\s*((CreationDate|ClosedDate|DeletionDate|Score|OwnerUserId),\\s*|(AnswerCount|Tag)$))"

  def isHeader(row: String): Boolean = row.matches(headerRegex)

}
