package it.unibo.bd18.stacklite.spark

import it.unibo.bd18.app.SparkApp
import it.unibo.bd18.stacklite.C.hdfs
import it.unibo.bd18.stacklite.Utils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

private[spark] trait StackliteSQLApp extends SparkApp {

  protected[this] final lazy val questionsDF = parquet.load(parquet.tables.questions, hdfs.data.questions, StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("creationDate", TimestampType, nullable = false),
      StructField("closedDate", TimestampType, nullable = true),
      StructField("deletionDate", TimestampType, nullable = true),
      StructField("score", IntegerType, nullable = false),
      StructField("ownerUserId", IntegerType, nullable = true),
      StructField("answerCount", IntegerType, nullable = true)
    )
  )).na.fill(0, Seq("answerCount"))

  protected[this] lazy val questionTagsDF = parquet.load(parquet.tables.questionTags, hdfs.data.questionTags, StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    )
  ))

  private[this] object parquet {
    private val basePath = hdfs.cache.job2.basePath

    object tables {
      val questions = "questions"

      val questionTags = "questionTags"
    }

    def load(tableName: String, path: String, schema: StructType): DataFrame = {
      val tablePath = s"${parquet.basePath}/$tableName.parquet"

      if (fs.exists(new Path(tablePath)))
        return spark.read
          .schema(schema)
          .parquet(tablePath)

      val basePath = new Path(parquet.basePath)
      if (!fs.exists(basePath))
        fs.mkdirs(basePath)

      val df = spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("timestampFormat", Utils.dateFormat)
        .option("nullValue", "NA")
        .load(path)

      df.write.parquet(tablePath)

      df
    }
  }

}
