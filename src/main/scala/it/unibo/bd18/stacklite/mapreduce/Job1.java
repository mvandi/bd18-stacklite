package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.QuestionTagData;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.CompositeJob;
import it.unibo.bd18.util.Pair;
import it.unibo.bd18.util.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static it.unibo.bd18.stacklite.Utils.df;

/**
 * Determine the five tags that received the highest sum of scores for each
 * year-month pair (tags sorted in descending order).
 */
public final class Job1 extends Configured implements Tool {

    private static abstract class AbstractRowMapper<T> extends Mapper<LongWritable, Text, IntWritable, ObjectWritable> {
        private final IntWritable keyOut = new IntWritable();
        private final ObjectWritable valueOut = new ObjectWritable();

        @Override
        protected final void map(LongWritable keyIn, Text valueIn, Context context) throws IOException, InterruptedException {
            final String row = valueIn.toString();
            if (!row.isEmpty() && !Utils.isHeader(row)) {
                final T t = mapper(row);
                if (filter(t)) {
                    keyOut.set(classifier(t));
                    valueOut.set(mapper(t));
                    context.write(keyOut, valueOut);
                }
            }
        }

        protected abstract T mapper(String row);

        protected boolean filter(T t) {
            return true;
        }

        protected abstract Writable mapper(T t);

        protected abstract int classifier(T t);
    }

    public static final class QuestionMapper extends AbstractRowMapper<QuestionData> {
        private static final Date startDate;
        private static final Date endDate;

        static {
            try {
                startDate = df.parse("2012-01-01T00:00:00Z");
                endDate = df.parse("2012-12-31T23:59:59Z");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected QuestionData mapper(String row) {
            return QuestionData.create(row);
        }

        @Override
        protected boolean filter(QuestionData questionData) {
            return Utils.between(questionData.creationDate(), startDate, endDate);
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

    public static final class QuestionTagMapper extends AbstractRowMapper<QuestionTagData> {
        @Override
        protected QuestionTagData mapper(String row) {
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

    public static class Combiner extends Reducer<IntWritable, ObjectWritable, Text, Text> {
        private final Text keyOut = new Text();
        private final Text valueOut = new Text();

        @Override
        protected void reduce(IntWritable keyIn, Iterable<ObjectWritable> valuesIn, Context context) throws IOException, InterruptedException {
            QuestionData question = null;
            final List<String> pendingTags = new ArrayList<>();

            for (final ObjectWritable value : valuesIn) {
                if (Utils.isInstanceOf(value.getDeclaredClass(), QuestionWritable.class)) {
                    if (question != null)
                        throw new IllegalStateException("Multiple questions for key " + keyIn);
                    question = ((QuestionWritable) value.get()).get();
                    keyOut.set(Utils.format(question.creationDate()));
                } else if (Utils.isInstanceOf(value.getDeclaredClass(), QuestionTagWritable.class)) {
                    final String tag = ((QuestionTagWritable) value.get()).get().tag();
                    if (question == null) {
                        pendingTags.add(tag);
                    } else {
                        final int score = question.score();
                        if (!pendingTags.isEmpty()) {
                            final Iterator<String> it = pendingTags.iterator();
                            while (it.hasNext()) {
                                write(context, it.next(), score);
                                it.remove();
                            }
                        }
                        write(context, tag, score);
                    }
                }
            }
        }

        private void write(Context context, String tag, int score) throws IOException, InterruptedException {
            valueOut.set(TextIntPairWritable.format(tag, score));
            context.write(keyOut, valueOut);
        }
    }

    public static final class InputMapper extends Mapper<Text, Text, Text, TextIntPairWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, TextIntPairWritable.create(value));
        }
    }

    public static final class Finisher extends Reducer<Text, TextIntPairWritable, Text, Text> {
        private final Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<TextIntPairWritable> values, Context context) throws IOException, InterruptedException {
            final Map<String, Integer> tags = new HashMap<>();
            for (final PairWritable<Text, IntWritable> value : values) {
                final Pair<Text, IntWritable> t = value.get();
                final String tag = t.left().toString();
                final int score = t.right().get();
                if (tags.containsKey(tag)) {
                    tags.put(tag, tags.get(tag) + score);
                } else {
                    tags.put(tag, score);
                }
            }
            final List<Pair<String, Integer>> result = Utils.sortedByValues(tags, false).subList(0, 5);
            valueOut.set(result.toString());
            context.write(key, valueOut);
        }
    }

    private Job createJob1(Configuration conf, Path questionsPath, Path questionTagsPath, Path outputPath) throws IOException {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ObjectWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, questionsPath, TextInputFormat.class, QuestionMapper.class);
        MultipleInputs.addInputPath(job, questionTagsPath, TextInputFormat.class, QuestionTagMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Combiner.class);

        return job;
    }

    private Job createJob2(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextIntPairWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        KeyValueTextInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(InputMapper.class);
        job.setReducerClass(Finisher.class);

        return job;
    }

    @Override
    public int run(String... args) throws Exception {
        final Path questionsPath = new Path(args[0]);
        final Path questionTagsPath = new Path(args[1]);
        final Path tempPath = new Path(args[2] + "-temp");
        final Path resultPath = new Path(args[2]);

        final Configuration conf = getConf();

        try (final FileSystem fs = FileSystem.get(conf)) {
            if (fs.exists(resultPath)) {
                fs.delete(resultPath, true);
            }

            final boolean succeeded = new CompositeJob()
                    .add(createJob1(conf, questionsPath, questionTagsPath, tempPath))
                    .add(createJob2(conf, tempPath, resultPath))
                    .waitForCompletion(true);

            fs.delete(tempPath, true);

            return succeeded ? 0 : 1;
        }
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Job1(), args));
    }

}
