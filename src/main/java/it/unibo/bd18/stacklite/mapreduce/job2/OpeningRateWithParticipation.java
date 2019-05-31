package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.C.job2;
import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.JobProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import java.util.Properties;

import static org.apache.hadoop.io.Text.Comparator;

/*
File in entrata: <tag, domanda>
-	Count tutte le domande
-	Count domande aperte
-	maxPartecipation = max (totalAnswers)
-	Sum(totalAnswers) di tutte le domande
-	Tasso di chiusura = domande aperte/totale domande
-	Partecipazione media = Sum(totalAnswers)/totale domande
-	Bassa = < maxPartecipation/3
-	Medio = beetween  maxPartecipation/3 and 2(maxPartecipation/3)
-	Alta = > 2(maxPartecipation/3)

Risultato:
-	<tag, tassoDiChiusura, partecipazioneMedia, discretizzazione>

 */
public final class OpeningRateWithParticipation implements JobProvider {

    private static final double LOW_THRESHOLD = 1.0 / 3.0;
    private static final double HIGH_THRESHOLD = 2.0 / 3.0;
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

        job.setSortComparatorClass(Comparator.class);

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
            final double questionCount = value.questionCount();
            final int totalAnswers = value.totalAnswers();

            final double openingRate = openQuestions / questionCount;
            final double averageParticipation = totalAnswers / questionCount;

            final Configuration conf = context.getConfiguration();
            final FileSystem fs = FileSystem.get(conf);
            final String filePath = conf.get("minmax.properties");
            final FSDataInputStream in = fs.open(new Path(filePath));
            final Properties props = new Properties();

            props.load(in);

            final double min = Double.parseDouble(props.getProperty("min"));
            final double max = Double.parseDouble(props.getProperty("max"));

            final String participation = discretize(averageParticipation, min, max);

            if (value.questionCount() > 1) {
                context.write(key, new Text(String.format("(%d,%d,%d,%f,%f,%s)",
                        openQuestions,
                        (int) questionCount,
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

    private static String discretize(double averageParticipation, double min, double max) {
        double normalization = (averageParticipation - min) / (max - min);

        if (normalization < LOW_THRESHOLD) {
            return "LOW";
        } else if (normalization > HIGH_THRESHOLD) {
            return "HIGH";
        }
        return "MEDIUM";
    }

}
