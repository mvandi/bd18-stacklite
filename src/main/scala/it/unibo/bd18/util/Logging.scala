package it.unibo.bd18.util

import org.apache.log4j.{LogManager, Logger}

trait Logging extends Serializable {

  @transient protected[this] lazy val log: Logger = LogManager.getLogger(getClass)

}
