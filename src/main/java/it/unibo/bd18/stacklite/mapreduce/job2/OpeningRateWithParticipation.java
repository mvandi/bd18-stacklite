package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.stacklite.mapreduce.job2.C.minmax;
import it.unibo.bd18.util.JobProvider;
import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static it.unibo.bd18.stacklite.mapreduce.job2.Utils.answerCount;
import static it.unibo.bd18.stacklite.mapreduce.job2.Utils.open;

public final class OpeningRateWithParticipation implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private final Path outputPath;

    public OpeningRateWithParticipation(Class<?> mainClass, Configuration conf, Path inputPath, Path outputPath) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    @Override
    public Job get() throws Exception {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapOutputValue.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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

    public static final class Finisher extends Reducer<Text, MapOutputValue, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<MapOutputValue> values, Context context) throws IOException, InterruptedException {
            final MapOutputValue value = sum(values);

            final int openQuestions = value.openQuestions();
            final int questionCount = value.questionCount();
            final int totalAnswers = value.totalAnswers();

            if (questionCount > 1) {
                final double openingRate = openQuestions / (double) questionCount;
                final double averageParticipation = totalAnswers / (double) questionCount;

                final Configuration conf = context.getConfiguration();
                final String minmaxPath = conf.get(minmax.properties.path);
                final FileSystem fs = FileSystem.get(conf);
                final InputStream in = fs.open(new Path(minmaxPath));

                final Properties props = new Properties();
                props.load(in);

                final double min = Double.parseDouble(props.getProperty(minmax.properties.min));
                final double max = Double.parseDouble(props.getProperty(minmax.properties.max));

                final String participation = discretize(averageParticipation, min, max);

                context.write(key, new Text(String.format("(%d,%d,%d,%f,%f,%s)",
                        openQuestions,
                        questionCount,
                        totalAnswers,
                        openingRate,
                        averageParticipation,
                        participation)));
            }
        }
    }

    private static MapOutputValue sum(Iterable<? extends MapOutputValue> values) {
        int openQuestions = 0;
        int questionCount = 0;
        int totalAnswers = 0;

        for (final MapOutputValue value : values) {
            openQuestions += value.openQuestions();
            questionCount += value.questionCount();
            totalAnswers += value.totalAnswers();
        }

        return MapOutputValue.create(openQuestions, questionCount, totalAnswers);
    }

    private static String discretize(double x, double min, double max) {
        final double normalized = normalize(x, min, max);

        final double lowThreshold = 1.0 / 3.0;
        final double highThreshold = 2.0 / 3.0;

        if (normalized < lowThreshold)
            return "LOW";
        if (normalized > highThreshold)
            return "HIGH";
        return "MEDIUM";
    }

    private static double normalize(double x, double min, double max) {
        return (x - min) / (max - min);
    }

    public static final class MapOutputValue extends TupleWritable {
        public static MapOutputValue create(Question question) {
            return new MapOutputValue(open(question), 1, answerCount(question));
        }

        public static MapOutputValue create(int openQuestions, int questionCount, int totalAnswers) {
            return new MapOutputValue(openQuestions, questionCount, totalAnswers);
        }

        public int openQuestions() {
            return get(0);
        }

        public int questionCount() {
            return get(1);
        }

        public int totalAnswers() {
            return get(2);
        }

        public MapOutputValue() {
            super();
        }

        private MapOutputValue(int openQuestions, int questionCount, int totalAnswers) {
            super(openQuestions, questionCount, totalAnswers);
        }
    }

}
