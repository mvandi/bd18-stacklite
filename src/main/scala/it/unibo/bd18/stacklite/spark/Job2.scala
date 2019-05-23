package it.unibo.bd18.stacklite.spark

object Job2 extends StackliteSQLApp {

  import it.unibo.bd18.stacklite.C.dates
  import it.unibo.bd18.stacklite.Utils
  import org.apache.hadoop.fs.Path
  import org.apache.spark.sql.functions.{count, sum, when}
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
    .orderBy("name")
    .select($"name", $"openQuestions", $"questionCount", $"totalAnswers",
      ($"openQuestions" / $"questionCount") * 100 as "openingRate",
      $"totalAnswers" / $"questionCount" as "averageParticipation")
    .write.parquet(resultPath)

}
