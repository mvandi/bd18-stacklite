package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.C.dates;
import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.QuestionTagData;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.JobProvider;
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

public abstract class AbstractJoin implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path questionsPath;
    private final Path questionTagsPath;
    private final Path outputPath;

    public AbstractJoin(Class<?> mainClass, Configuration conf, Path questionsPath, Path questionTagsPath, Path outputPath) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.questionsPath = questionsPath;
        this.questionTagsPath = questionTagsPath;
        this.outputPath = outputPath;
    }

    @Override
    public final Job get() throws IOException {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ObjectWritable.class);
        job.setOutputKeyClass(getOutputKeyClass());
        job.setOutputValueClass(getOutputValueClass());

        MultipleInputs.addInputPath(job, questionsPath, TextInputFormat.class, getQuestionMapperClass());
        MultipleInputs.addInputPath(job, questionTagsPath, TextInputFormat.class, getQuestionTagMapperClass());
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setReducerClass(getFinisherClass());

        return job;
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

    public static class QuestionMapperBase extends AbstractRowMapper<QuestionData> {
        @Override
        protected final QuestionData mapper(String row) {
            return QuestionData.create(row);
        }

        @Override
        protected boolean filter(QuestionData question) {
            return Utils.between(question.creationDate(), dates.startDate, dates.endDate);
        }

        @Override
        protected final QuestionWritable mapper(QuestionData question) {
            return QuestionWritable.create(question);
        }

        @Override
        protected final int classifier(QuestionData question) {
            return question.id();
        }
    }

    public static class QuestionTagMapperBase extends AbstractRowMapper<QuestionTagData> {
        @Override
        protected final QuestionTagData mapper(String row) {
            return QuestionTagData.create(row);
        }

        @Override
        protected final QuestionTagWritable mapper(QuestionTagData tag) {
            return QuestionTagWritable.create(tag);
        }

        @Override
        protected final int classifier(QuestionTagData tag) {
            return tag.id();
        }
    }

    protected Class<? extends QuestionMapperBase> getQuestionMapperClass() {
        return QuestionMapperBase.class;
    }

    protected Class<? extends QuestionTagMapperBase> getQuestionTagMapperClass() {
        return QuestionTagMapperBase.class;
    }

    protected abstract Class<? extends Reducer> getFinisherClass();

    protected abstract Class<?> getOutputKeyClass();

    protected abstract Class<?> getOutputValueClass();

}
