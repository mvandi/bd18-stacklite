package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.QuestionTagData;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.JobProvider;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static it.unibo.bd18.stacklite.C.dates.endDate;
import static it.unibo.bd18.stacklite.C.dates.startDate;

public final class Join {

    public static JobProvider create(final Class<?> mainClass, final Configuration conf, final Path questionsPath, final Path questionTagsPath, final Path outputPath) {
        return new JobProvider() {
            @Override
            public Job get() throws IOException {
                final Job job = Job.getInstance(conf);

                job.setJarByClass(mainClass);

                job.setMapOutputKeyClass(IntWritable.class);
                job.setMapOutputValueClass(ObjectWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, questionsPath, TextInputFormat.class, QuestionMapper.class);
                MultipleInputs.addInputPath(job, questionTagsPath, TextInputFormat.class, QuestionTagMapper.class);
                FileOutputFormat.setOutputPath(job, outputPath);

                job.setReducerClass(Combiner.class);

                return job;
            }
        };
    }

    private static abstract class AbstractRowMapper<T> extends Mapper<LongWritable, Text, IntWritable, ObjectWritable> {
        private final IntWritable keyOut = new IntWritable();
        private final ObjectWritable valueOut = new ObjectWritable();

        @Override
        protected final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String row = value.toString();
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

        @Override
        protected void reduce(IntWritable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            int score = 0;
            boolean scoreAssigned = false;
            final List<String> pendingTags = new ArrayList<>();

            for (final ObjectWritable value : values) {
                Class valueClass = value.getDeclaredClass();
                if (ClassUtils.isAssignable(valueClass, QuestionWritable.class)) {
                    if (scoreAssigned)
                        throw new IllegalStateException("Multiple questions for key " + key);
                    final QuestionData question = ((QuestionWritable) value.get()).get();
                    keyOut.set(Utils.format(question.creationDate()));
                    score = question.score();
                    final Iterator<String> it = pendingTags.iterator();
                    while (it.hasNext()) {
                        write(context, it.next(), score);
                        it.remove();
                    }
                    scoreAssigned = true;
                } else if (ClassUtils.isAssignable(valueClass, QuestionTagWritable.class)) {
                    final String tag = ((QuestionTagWritable) value.get()).get().tag();
                    if (scoreAssigned)
                        write(context, tag, score);
                    else
                        pendingTags.add(tag);
                }
            }
        }

        private void write(Context context, String tag, int score) throws IOException, InterruptedException {
            final Text valueOut = new Text(TextIntPairWritable.format(tag, score));
            context.write(keyOut, valueOut);
        }
    }

    private Join() {
    }

}
