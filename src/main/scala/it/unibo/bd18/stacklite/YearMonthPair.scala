package it.unibo.bd18.stacklite

import java.util.{Calendar, Date}

sealed trait YearMonthPair {

  def year: Int

  def month: Int

  override def toString: String = YearMonthPair.format(year, month)

}

object YearMonthPair {

  def apply(d: Date): YearMonthPair = {
    val (year, month) = tupled(d)
    YearMonthPairImpl(year,  month)
  }

  def format(d: Date): String = {
    val (year, month) = tupled(d)
    format(year, month)
  }

  private def format(year: Int, month: Int): String = s"($year, $month)"

  private def tupled(d: Date): (Int, Int) = {
    val c = Calendar.getInstance()
    c.setTime(d)
    (c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1)
  }

  private case class YearMonthPairImpl(
                                        override val year: Int,
                                        override val month: Int
                                      ) extends YearMonthPair

}
