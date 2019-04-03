package it.unibo.bd18.util

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.Tool

abstract class ConfiguredTool(private val jobName: String) extends Configured with Tool {

  protected def setupJob(job: Job, args: Array[String]): Unit

  override final def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, jobName)

    setupJob(job, args)

    if (job.waitForCompletion(true)) 1 else 0
  }

}
