package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.QuestionTagData;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Job1 extends Configured implements Tool {

    private static abstract class AbstractRowAccumulator<T> extends Mapper<Object, Text, IntWritable, ObjectWritable> {
        @Override
        protected final void map(Object ignore, Text value, Context context) throws IOException, InterruptedException {
            final String row = value.toString();
            if (!row.isEmpty() && !Utils.isHeader(row)) {
                final T t = mapper(row.split("\\s*,\\s*"));
                context.write(new IntWritable(classifier(t)), new ObjectWritable(mapper(t)));
            }
        }

        protected abstract T mapper(String[] row);

        protected abstract Writable mapper(T t);

        protected abstract int classifier(T t);
    }

    public static final class QuestionAccumulator extends AbstractRowAccumulator<QuestionData> {
        @Override
        protected QuestionData mapper(String[] row) {
            return QuestionData.create(row);
        }

        @Override
        protected QuestionWritable mapper(QuestionData question) {
            return QuestionWritable.create(question);
        }

        @Override
        protected int classifier(QuestionData question) {
            return question.id();
        }
    }

    public static final class QuestionTagAccumulator extends AbstractRowAccumulator<QuestionTagData> {
        @Override
        protected QuestionTagData mapper(String[] row) {
            return QuestionTagData.create(row);
        }

        @Override
        protected QuestionTagWritable mapper(QuestionTagData tag) {
            return QuestionTagWritable.create(tag);
        }

        @Override
        protected int classifier(QuestionTagData tag) {
            return tag.id();
        }
    }

    public static class Combiner extends Reducer<IntWritable, ObjectWritable, Text, TextIntWritablePair> {
        @Override
        protected void reduce(IntWritable ignore, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            QuestionData question = null;
            final List<String> tags = new ArrayList<>();

            for (final ObjectWritable value : values) {
                if (Utils.isInstanceOf(value.getDeclaredClass(), QuestionWritable.class)) {
                    if (question != null)
                        throw new IllegalStateException("Multiple questions for key " + ignore);
                    question = ((QuestionWritable) value.get()).get();
                } else if (Utils.isInstanceOf(value.getDeclaredClass(), QuestionTagWritable.class)) {
                    tags.add(((QuestionTagWritable) value.get()).get().tag());
                }
            }

            if (question != null && !tags.isEmpty()) {
                final Text key = new Text(Utils.format(question.creationDate()));

                final IntWritable score = new IntWritable(question.score());

                for (final String tag : tags) {
                    context.write(key, TextIntWritablePair.create(new Text(tag), score));
                }
            }
        }
    }

    public static class Finisher extends Reducer<Text, TextIntWritablePair, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<TextIntWritablePair> values, Context context) throws IOException, InterruptedException {
            final Map<String, Integer> tags = new HashMap<>();
            for (final PairWritable<Text, IntWritable> value : values) {
                final Pair<Text, IntWritable> t = value.get();
                final String tag = t.left().toString();
                final int score = t.right().get();
                if (tags.containsKey(tag)) {
                    final int oldScore = tags.get(tag);
                    tags.put(tag, oldScore + score);
                } else {
                    tags.put(tag, score);
                }
            }
            final List<Pair<String, Integer>> result = Utils.sortedByValues(tags).subList(0, 5);
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

        MultipleInputs.addInputPath(job, questionsFile, TextInputFormat.class, QuestionAccumulator.class);
        MultipleInputs.addInputPath(job, questionTagsFile, TextInputFormat.class, QuestionTagAccumulator.class);
        FileOutputFormat.setOutputPath(job, resultPath);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Finisher.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Job1(), args));
    }

}
