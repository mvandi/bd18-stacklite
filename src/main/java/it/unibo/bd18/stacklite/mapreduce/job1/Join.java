package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.stacklite.mapreduce.AbstractJoin;
import it.unibo.bd18.stacklite.mapreduce.QuestionTagWritable;
import it.unibo.bd18.stacklite.mapreduce.QuestionWritable;
import it.unibo.bd18.stacklite.mapreduce.TextIntWritable;
import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class Join extends AbstractJoin {

    public Join(Class<?> mainClass, Configuration conf, Path questionsPath, Path questionTagsPath, Path outputPath) {
        super(mainClass, conf, questionsPath, questionTagsPath, outputPath);
    }

    @Override
    protected Class<? extends Reducer> getReducerClass() {
        return Combiner.class;
    }

    @Override
    protected Class<?> getOutputKeyClass() {
        return Text.class;
    }

    @Override
    protected Class<?> getOutputValueClass() {
        return Text.class;
    }

    public static class Combiner extends Reducer<IntWritable, ObjectWritable, Text, Text> {
        private final Text keyOut = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            int score = 0;
            boolean scoreAssigned = false;
            final List<String> pendingTags = new LinkedList<>();

            for (final ObjectWritable value : values) {
                final Class valueClass = value.getDeclaredClass();
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
                    pendingTags.clear();
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
            final Text valueOut = new Text(TextIntWritable.format(tag, score));
            context.write(keyOut, valueOut);
        }
    }

}

