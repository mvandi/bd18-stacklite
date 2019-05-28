package it.unibo.bd18.stacklite.spark

import org.apache.spark.SparkConf

import scala.language.postfixOps

object Job2 extends StackliteSQLApp {

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job2")

  import java.util.Date

  import it.unibo.bd18.stacklite.C.dates
  import it.unibo.bd18.stacklite.Utils
  import org.apache.hadoop.fs.Path
  import org.apache.spark.sql.Column
  import org.apache.spark.sql.functions._
  import spark.implicits._

  @inline def d(d: Date): String = Utils.toString(d)

  val resultPath = args(0)
  Utils.deleteIfExists(fs, true, new Path(resultPath))

  val baseDF = questionsDF
    .where(($"creationDate" between(d(dates.startDate), d(dates.endDate)))
      && ($"deletionDate" isNull))
    .join(questionTagsDF, "id")
    .withColumn("open", when($"closedDate" isNull, 1) otherwise 0)
    .groupBy("name")
    .agg(
      sum("open") as "openQuestions",
      count("*") as "questionCount",
      sum("answerCount") as "totalAnswers")
    .where($"questionCount" > 1)
    .withColumn("averageParticipation", $"totalAnswers" / $"questionCount")
    .cache()

  baseDF.crossJoin(baseDF
    .select(
      min($"averageParticipation") as "minParticipation",
      max($"averageParticipation") as "maxParticipation"))
    .select(
      $"name",
      $"openQuestions",
      $"questionCount",
      $"totalAnswers",
      ($"openQuestions" / $"questionCount") as "openingRate",
      $"averageParticipation",
      discretize($"averageParticipation", $"minParticipation", $"maxParticipation") as "participation")
    .write.parquet(resultPath)

  //resultDF.explain(extended = true)

  private def discretize(x: Column, min: Column, max: Column): Column = {
    val normalized = normalize(x, min, max)

    val lowThreshold = 1.0 / 3.0
    val highThreshold = 2.0 / 3.0

    when(normalized < lowThreshold, "LOW")
      .when(normalized > highThreshold, "HIGH")
      .otherwise("MEDIUM")
  }

  private def normalize(x: Column, min: Column, max: Column) = (x - min) / (max - min)

}
