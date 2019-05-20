package it.unibo.bd18.stacklite.spark

import org.apache.spark.SparkConf

object Job2 extends StackliteSQLApp {

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job2")

  import java.util.Date

  import it.unibo.bd18.stacklite.C.dates
  import it.unibo.bd18.stacklite.Utils
  import org.apache.hadoop.fs.Path
  import org.apache.spark.sql.functions.{count, sum, when}
  import spark.implicits._

  def d(d: Date): String = Utils.toString(d)

  val resultPath = args(0)
  Utils.deleteIfExists(fs, true, new Path(resultPath))

  questionsDF
    .where(($"creationDate" between(d(dates.startDate), d(dates.endDate)))
      && ($"deletionDate" isNull))
    .join(questionTagsDF, "id")
    .withColumn("open", when($"closedDate" isNull, 1) otherwise 0)
    .groupBy("name")
    .agg(
      sum("open") as "openQuestions",
      count("*") as "questionCount",
      sum("answerCount") as "totalAnswers")
    .orderBy($"name" asc)
    .select(
      $"name",
      $"openQuestions",
      $"questionCount",
      $"totalAnswers",
      ($"openQuestions" / $"questionCount") * 100 as "openingRate",
      ($"totalAnswers" / $"questionCount") as "averageParticipation")
    .write.parquet(resultPath)

  //resultDF.explain(extended = true)

}
