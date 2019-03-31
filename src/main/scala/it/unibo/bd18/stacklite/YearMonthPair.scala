package it.unibo.bd18.stacklite

import java.util.{Calendar, Date}

sealed trait YearMonthPair {

  def month(): Int

  def year(): Int

  override def toString: String = s"($year, $month)"

}

object YearMonthPair {

  def apply(d: Date): YearMonthPair = create(d)

  def create(d: Date): YearMonthPair = {
    val c = Calendar.getInstance()
    c.setTime(d)
    YearMonthPairImpl(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1)
  }

  private case class YearMonthPairImpl(
                                        override val month: Int,
                                        override val year: Int
                                      ) extends YearMonthPair {

  }

}
