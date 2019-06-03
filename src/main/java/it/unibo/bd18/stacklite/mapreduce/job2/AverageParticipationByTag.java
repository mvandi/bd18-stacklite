package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.JobProvider;
import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static it.unibo.bd18.stacklite.mapreduce.job2.Job2Utils.answerCount;

public final class AverageParticipationByTag implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private final Path outputPath;

    public AverageParticipationByTag(Class mainClass, Configuration conf, Path tempPath, Path outputPath) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.inputPath = tempPath;
        this.outputPath = outputPath;
    }

    @Override
    public Job get() throws Exception {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapOutputValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, inputPath, KeyValueTextInputFormat.class, InputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Finisher.class);

        return job;
    }

    public static final class InputMapper extends Mapper<Text, Text, Text, MapOutputValue> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            final Question question = Question.create(value);
            context.write(key, MapOutputValue.create(question));
        }
    }

    public static final class Combiner extends Reducer<Text, MapOutputValue, Text, MapOutputValue> {
        @Override
        protected void reduce(Text key, Iterable<MapOutputValue> values, Context context) throws IOException, InterruptedException {
            context.write(key, sum(values));
        }
    }

    public static final class Finisher extends Reducer<Text, MapOutputValue, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<MapOutputValue> values, Context context) throws IOException, InterruptedException {
            final MapOutputValue value = sum(values);

            final int questionCount = value.questionCount();
            final int totalAnswers = value.totalAnswers();

            if (questionCount > 1) {
                final double averageParticipation = totalAnswers / (double) questionCount;
                context.write(key, new DoubleWritable(averageParticipation));
            }
        }
    }

    private static MapOutputValue sum(Iterable<? extends MapOutputValue> values) {
        int questionCount = 0;
        int totalAnswers = 0;

        for (final MapOutputValue value : values) {
            questionCount += value.questionCount();
            totalAnswers += value.totalAnswers();
        }

        return MapOutputValue.create(questionCount, totalAnswers);
    }

    public static class MapOutputValue extends TupleWritable {

        public static MapOutputValue create(Question question) {
            return new MapOutputValue(1, answerCount(question));
        }

        public static MapOutputValue create(int questionCount, int totalAnswers) {
            return new MapOutputValue(questionCount, totalAnswers);
        }

        public int questionCount() {
            return get(0);
        }

        public int totalAnswers() {
            return get(1);
        }

        public MapOutputValue() {
            super();
        }

        private MapOutputValue(int questionCount, int totalAnswers) {
            super(questionCount, totalAnswers);
        }
    }

}
