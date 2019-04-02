package it.unibo.bd18.stacklite.mapreduce

import java.lang

import it.unibo.bd18.stacklite._
import it.unibo.bd18.util.ConfiguredTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, MultipleInputs}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.ToolRunner

import scala.collection.JavaConversions._

/**
  * Determine the five tags that received the highest sum of scores for each
  * year-month pair (tags are sorted in descending order).
  */
object Job1 extends ConfiguredTool("Job1") with App {

  import it.unibo.bd18.util.implicits._

  protected abstract class AbstractAccumulator[T] extends Mapper[Any, Text, IntWritable, ObjectWritable] {
    protected override def map(key: Any,
                               value: Text,
                               context: Mapper[Any, Text, IntWritable, ObjectWritable]#Context): Unit =
      Some(value.toString)
        .filterNot(Utils.isHeader)
        .map(_.split("\\s*,\\s*"))
        .map(converter)
        .foreach(t => context.write(new IntWritable(keyExtractor(t)), new ObjectWritable(t)))

    protected def converter(row: Array[String]): T

    protected def keyExtractor(t: T): Int
  }

  class QuestionAccumulator extends AbstractAccumulator[QuestionData] {
    protected override def converter(row: Array[String]): QuestionData = QuestionData(row)

    protected override def keyExtractor(question: QuestionData): Int = question.id
  }

  class QuestionTagAccumulator extends AbstractAccumulator[QuestionTagData] {
    protected override def converter(row: Array[String]): QuestionTagData = QuestionTagData(row)

    protected override def keyExtractor(tag: QuestionTagData): Int = tag.id
  }

  class Combiner extends Reducer[IntWritable, ObjectWritable, Text, ObjectWritable] {
    protected override def reduce(key: IntWritable,
                        values: lang.Iterable[ObjectWritable],
                        context: Reducer[IntWritable, ObjectWritable, Text, ObjectWritable]#Context): Unit =
      values.find(_.getDeclaredClass == classOf[QuestionData])
        .map(_.get.asInstanceOf[QuestionData])
        .fold(throw new IllegalStateException(s"question for key `${key.get}` is not defined")) { question =>
          val newKey = new Text(YearMonthPair.format(question.creationDate))
          val score = question.score
          values.toStream
            .filter(_.getDeclaredClass == classOf[QuestionTagData])
            .map(_.get.asInstanceOf[QuestionTagData].tag)
            .foreach(tag => context.write(newKey, new ObjectWritable((tag, score))))
        }
  }

  class Finisher extends Reducer[Text, ObjectWritable, Text, Text] {
    protected override def reduce(
                                   key: Text,
                                   values: lang.Iterable[ObjectWritable],
                                   context: Reducer[Text, ObjectWritable, Text, Text]#Context): Unit =
      context.write(key, new Text(values.toStream
      .map(_.get.asInstanceOf[(String, Int)])
      .groupByKey
      .mapValues(_.sum)
      .toStream
      .sortBy(-_._2)
      .map(_._1)
      .take(5)
      .mkString("[", ", ", "]")))
  }

  override protected def setupJob(job: Job, jobArgs: Array[String]): Unit = {
    val questionsFile = new Path(jobArgs(0))
    val questionTagsFile = new Path(jobArgs(1))
    val resultPath = new Path(jobArgs(2))

    val conf = job.getConfiguration

    val fs = FileSystem.get(conf)
    if (fs exists resultPath) {
      fs.delete(resultPath, true)
    }

    job.setJarByClass(getClass)

    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[ObjectWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    MultipleInputs.addInputPath(job, questionsFile, classOf[FileInputFormat[Any, Text]], classOf[QuestionAccumulator])
    MultipleInputs.addInputPath(job, questionTagsFile, classOf[FileInputFormat[Any, Text]], classOf[QuestionTagAccumulator])
    FileOutputFormat.setOutputPath(job, resultPath)

    job.setCombinerClass(classOf[Combiner])
    job.setReducerClass(classOf[Finisher])
  }

  System.exit(ToolRunner.run(new Configuration(), Job1, args))

}
