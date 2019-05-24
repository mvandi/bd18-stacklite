package it.unibo.bd18.stacklite.spark

import org.apache.spark.sql.Column

object Job2 extends StackliteSQLApp {

  import it.unibo.bd18.stacklite.C.dates
  import it.unibo.bd18.stacklite.Utils
  import org.apache.hadoop.fs.Path
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val resultPath = args(0)
  Utils.deleteIfExists(fs, true, new Path(resultPath))

  val resultDF = questionsDF
    .where($"creationDate" between(Utils.toString(dates.startDate), Utils.toString(dates.endDate)) and ($"deletionDate" isNull))
    .join(questionTagsDF, "id")
    .withColumn("openQuestions", when($"closedDate".isNull, 1) otherwise 0)
    .groupBy("name")
    .agg(count("*") as "questionCount",
      sum("answerCount") as "totalAnswers",
      sum("openQuestions") as "openQuestions")
    .cache()

  resultDF.crossJoin(resultDF
    .select(min("totalAnswers") as "minParticipation",
      max("totalAnswers") as "maxParticipation"))
    .select(
      $"name",
      $"openQuestions",
      $"questionCount",
      $"totalAnswers",
      ($"openQuestions" / $"questionCount") * 100 as "openingRate",
      ($"totalAnswers" / $"questionCount") as "averageParticipation",
      discretize($"totalAnswers", $"minParticipation", $"maxParticipation") as "participation")
    .write.parquet(resultPath)

  private def discretize(totalAnswers: Column, minParticipation: Column, maxParticipation: Column): Column = {
    val normalization = totalAnswers - minParticipation / maxParticipation - minParticipation
    when(normalization < 0.33, "LOW").when(normalization > 0.66, "HIGH") otherwise "MEDIUM"
  }

}
