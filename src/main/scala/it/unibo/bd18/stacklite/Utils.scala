package it.unibo.bd18.stacklite

import java.util
import java.util.Comparator
import java.util.Map.Entry

import scala.collection.JavaConverters._

object Utils {

  private val headerRegex = "(^Id,\\s*|,\\s*((CreationDate|ClosedDate|DeletionDate|Score|OwnerUserId),\\s*|(AnswerCount|Tag)$))"

  def isHeader(row: String): Boolean = row.matches(headerRegex)

  def sortedKeysByValues[K, V <: Comparable[V]](m: util.Map[K, V]): util.List[K] = sortedKeysByValues(m, ascending = true)

  def sortedKeysByValues[K, V <: Comparable[V]](m: util.Map[K, V], ascending: Boolean): util.List[K] = {
    val entries = new util.ArrayList[Entry[K, V]](m.entrySet)
    entries.sort(new Comparator[Entry[K, V]] {
      private val asc = if (ascending) 1 else -1

      override def compare(o1: Entry[K, V], o2: Entry[K, V]): Int = asc * o1.getValue.compareTo(o2.getValue)
    })

    val result = new util.ArrayList[K]()
    for (entry <- entries.asScala) {
      result.add(entry.getKey)
    }
    result
  }

}
