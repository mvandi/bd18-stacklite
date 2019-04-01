package it.unibo.bd18.stacklite.mapreduce

import java.lang

import it.unibo.bd18.stacklite._
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

//remove if not needed
import scala.collection.JavaConversions._

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (tags are sorted in descending order).
  */
object Job1 extends Configured with Tool with App {

  abstract class AbstractRowMapper[T] extends Mapper[Any, Text, IntWritable, ObjectWritable] {
    protected override def map(ignore: Any,
                               value: Text,
                               context: Mapper[Any, Text, IntWritable, ObjectWritable]#Context): Unit = {
      val row = value.toString
      if (!Utils.isHeader(row)) {
        val t = converter(row.split("\\s*,\\s*"))
        context.write(new IntWritable(keyExtractor(t)), new ObjectWritable(t))
      }
    }

    protected def converter(row: Array[String]): T

    protected def keyExtractor(t: T): Int
  }

  class QuestionMapper extends AbstractRowMapper[QuestionData] {
    protected override def converter(row: Array[String]): QuestionData = QuestionData(row)

    protected override def keyExtractor(question: QuestionData): Int = question.id
  }

  class QuestionTagMapper extends AbstractRowMapper[QuestionTagData] {
    protected override def converter(row: Array[String]): QuestionTagData = QuestionTagData(row)

    protected override def keyExtractor(tag: QuestionTagData): Int = tag.id
  }

  class Combiner extends Reducer[IntWritable, ObjectWritable, Text, ObjectWritable] {
    override def reduce(ignore: IntWritable,
                        values: lang.Iterable[ObjectWritable],
                        context: Reducer[IntWritable, ObjectWritable, Text, ObjectWritable]#Context): Unit = {
      var question: QuestionData = null
      val tags = new collection.mutable.ListBuffer[String]()
      for (value <- values) {
        val valueClass = value.getDeclaredClass
        if (valueClass == classOf[QuestionData]) {
          if (question != null)
            throw new IllegalStateException(s"Multiple questions for key $ignore")
          question = value.asInstanceOf[QuestionData]
        } else if (valueClass == classOf[QuestionTagData]) {
          tags += value.get.asInstanceOf[QuestionTagData].tag
        }
      }
      if (question != null && tags.nonEmpty) {
        val key = new Text(YearMonthPair.format(question.creationDate))
        val score = question.score
        tags foreach(tag => context.write(key, new ObjectWritable((tag, score))))
      }
    }
  }

  class Finisher extends Reducer[Text, ObjectWritable, Text, Text] {
    override def reduce(key: Text,
                        values: lang.Iterable[ObjectWritable],
                        context: Reducer[Text, ObjectWritable, Text, Text]#Context): Unit = {
      val tags = new collection.mutable.HashMap[String, Integer]()
      for (value <- values) {
        val (tag, score) = value.get.asInstanceOf[(String, Integer)]
        if (tags containsKey tag) {
          tags(tag) = tags(tag) + score
        } else {
          tags(tag) = score
        }
      }

      context.write(key, new Text(tags.toStream
        .sortBy(-_._2)
        .map(_._1)
        .take(5)
        .toList
        .mkString("[", ", ", "]")))
    }
  }

  override def run(args: Array[String]): Int = {
    val questionsFile = new Path(args(0))
    val questionTagsFile = new Path(args(1))
    val resultPath = new Path(args(2))

    val conf = getConf

    val fs = FileSystem.get(conf)
    if (fs.exists(resultPath)) {
      fs.delete(resultPath, true)
    }

    val job = Job.getInstance(conf, "Job1")

    job.setJarByClass(getClass)

    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[ObjectWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    MultipleInputs.addInputPath(job,questionsFile,classOf[FileInputFormat[Any, Text]],classOf[QuestionMapper])
    MultipleInputs.addInputPath(job, questionTagsFile, classOf[FileInputFormat[Any, Text]], classOf[QuestionTagMapper])
    FileOutputFormat.setOutputPath(job, resultPath)

    job.setCombinerClass(classOf[Combiner])
    job.setReducerClass(classOf[Finisher])

    if (job.waitForCompletion(true)) 1 else 0
  }

  System.exit(ToolRunner.run(new Configuration(), Job1, args))

}
