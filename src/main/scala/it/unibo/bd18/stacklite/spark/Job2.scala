package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.stacklite.C
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._ // for `when`

object Job2 extends StackliteSQLApp {

  override protected[this] val conf: SparkConf = new SparkConf().setAppName("Job2")

  import spark.implicits._ // $""

  val resultDF = questionsDF
    .where($"creationDate" between(C.dates.startDate, C.dates.endDate) and ($"closingDate" isNull))
    .join(questionTagsDF, "id")
    .withColumn("openQuestions", when($"closedDate".isNull, 1) otherwise 0)
    .groupBy("name")
    .agg(count("*") as "questionCount",
      sum("answerCount") as "totalAnswers",
      sum("openQuestions"))
    .orderBy("name")
    .select($"name", $"openQuestions", $"questionCount", $"totalAnswers",
      ($"openQuestions" / $"questionCount") * 100 as "openingRate",
      $"totalAnswers" / $"questionCount" as "averageParticipation")
    .show(10)

}
