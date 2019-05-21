package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.C.dates;
import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.stacklite.QuestionTag;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
    public final Job get() throws Exception {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(ObjectWritable.class);
        job.setOutputKeyClass(getOutputKeyClass());
        job.setOutputValueClass(getOutputValueClass());

        MultipleInputs.addInputPath(job, questionsPath, TextInputFormat.class, getQuestionMapperClass());
        MultipleInputs.addInputPath(job, questionTagsPath, TextInputFormat.class, getQuestionTagMapperClass());
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setReducerClass(getJoinerClass());

        return job;
    }

    protected Class<? extends QuestionMapperBase> getQuestionMapperClass() {
        return QuestionMapperBase.class;
    }

    protected Class<? extends QuestionTagMapperBase> getQuestionTagMapperClass() {
        return QuestionTagMapperBase.class;
    }

    protected abstract Class<? extends JoinerBase> getJoinerClass();

    protected abstract Class<?> getOutputKeyClass();

    protected abstract Class<?> getOutputValueClass();

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

    public static class QuestionMapperBase extends AbstractRowMapper<Question> {
        @Override
        protected final Question mapper(String row) {
            return Question.create(row);
        }

        @Override
        protected boolean filter(Question question) {
            return Utils.between(question.creationDate(), dates.startDate, dates.endDate);
        }

        @Override
        protected final QuestionWritable mapper(Question question) {
            return QuestionWritable.create(question);
        }

        @Override
        protected final int classifier(Question question) {
            return question.id();
        }
    }

    public static class QuestionTagMapperBase extends AbstractRowMapper<QuestionTag> {
        @Override
        protected final QuestionTag mapper(String row) {
            return QuestionTag.create(row);
        }

        @Override
        protected final QuestionTagWritable mapper(QuestionTag tag) {
            return QuestionTagWritable.create(tag);
        }

        @Override
        protected final int classifier(QuestionTag tag) {
            return tag.id();
        }
    }

    public static abstract class JoinerBase<K, V> extends Reducer<IntWritable, ObjectWritable, K, V> {
        @Override
        protected final void reduce(IntWritable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            Question question = null;
            final List<QuestionTag> pendingTags = new LinkedList<>();

            preReduce();

            for (final ObjectWritable value : values) {
                final Class valueClass = value.getDeclaredClass();
                if (ClassUtils.isAssignable(valueClass, QuestionWritable.class)) {
                    if (question != null)
                        throw new IllegalStateException("Multiple questions for key " + key);
                    question = ((QuestionWritable) value.get()).get();
                    final Iterator<QuestionTag> it = pendingTags.iterator();
                    while (it.hasNext()) {
                        write(context, question, it.next());
                        it.remove();
                    }
                } else if (ClassUtils.isAssignable(valueClass, QuestionTagWritable.class)) {
                    final QuestionTag tag = ((QuestionTagWritable) value.get()).get();
                    if (question != null) {
                        write(context, question, tag);
                    } else
                        pendingTags.add(tag);
                }
            }

            postReduce();
        }

        private void write(Context context, Question question, QuestionTag tag) throws IOException, InterruptedException {
            final K key = computeOutputKey(question, tag);
            final V value = computeOutputValue(question, tag);
            context.write(key, value);
        }

        protected abstract K computeOutputKey(Question question, QuestionTag tag);

        protected abstract V computeOutputValue(Question question, QuestionTag tag);

        protected void preReduce() {
        }

        protected void postReduce() {
        }
    }

}
