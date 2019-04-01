package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Job1 extends Configured implements Tool {

    private static final QuestionData$ QuestionData = QuestionData$.MODULE$;
    private static final QuestionTagData$ QuestionTagData = QuestionTagData$.MODULE$;
    private static final YearMonthPair$ YearMonthPair = YearMonthPair$.MODULE$;

    public static class QuestionMapper extends Mapper<Object, Text, IntWritable, ObjectWritable> {
        @Override
        protected void map(Object ignore, Text value, Context context) throws IOException, InterruptedException {
            final String row = value.toString();
            if (!Utils.isHeader(row)) {
                final QuestionData q = QuestionData.create(row.split("\\s*,+\\s*"));
                context.write(new IntWritable(q.id()), new ObjectWritable(q));
            }
        }
    }

    public static class QuestionTagMapper extends Mapper<Object, Text, IntWritable, ObjectWritable> {
        @Override
        protected void map(Object ignore, Text value, Context context) throws IOException, InterruptedException {
            final String row = value.toString();
            if (!Utils.isHeader(row)) {
                final QuestionTagData qt = QuestionTagData.create(row.split("\\s*,+\\s*"));
                context.write(new IntWritable(qt.id()), new ObjectWritable(qt));
            }
        }
    }

    public static class Combiner extends Reducer<IntWritable, ObjectWritable, Text, ObjectWritable> {
        @Override
        protected void reduce(IntWritable ignore, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            QuestionData question = null;
            final List<String> tags = new ArrayList<>();

            for (final ObjectWritable value : values) {
                Class<?> valueClass = value.getDeclaredClass();
                if (valueClass == QuestionData.class) {
                    if (question != null)
                        throw new IllegalStateException("Multiple questions for key " + ignore);
                    question = (QuestionData) value;
                } else if (valueClass == QuestionTagData.class) {
                    tags.add(((QuestionTagData) value.get()).tag());
                }
            }

            if (question != null && !tags.isEmpty()) {
                final Text key = new Text(YearMonthPair.create(question.creationDate()).toString());

                final int score = question.score();

                for (final String tag : tags) {
                    context.write(key, new ObjectWritable(new Tuple2<>(tag, score)));
                }
            }
        }
    }

    public static class Finisher extends Reducer<Text, ObjectWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            final Map<String, Integer> tags = new HashMap<>();
            for (final ObjectWritable value : values) {
                final Tuple2<String, Integer> t = (Tuple2<String, Integer>) value.get();
                final String tag = t._1();
                final int score = t._2();
                if (tags.containsKey(tag)) {
                    final int oldScore = tags.get(tag);
                    tags.put(tag, oldScore + score);
                } else {
                    tags.put(tag, score);
                }
            }
            final List<String> result = Utils.sortedKeysByValues(tags).subList(0, 5);
            context.write(key, new Text(result.toString()));
        }
    }

    @Override
    public int run(String... args) throws Exception {
        final Path questionsFile = new Path(args[0]);
        final Path questionTagsFile = new Path(args[1]);
        final Path resultPath = new Path(args[2]);

        final Configuration conf = getConf();

        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(resultPath)) {
            fs.delete(resultPath, true);
        }

        final Job job = Job.getInstance(conf, "Job1");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ObjectWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, questionsFile, FileInputFormat.class, QuestionMapper.class);
        MultipleInputs.addInputPath(job, questionTagsFile, FileInputFormat.class, QuestionTagMapper.class);
        FileOutputFormat.setOutputPath(job, resultPath);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Combiner.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Job1(), args));
    }

}
