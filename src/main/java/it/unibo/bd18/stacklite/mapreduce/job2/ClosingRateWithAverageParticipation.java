package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.JobProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
File in entrata: <tag, domanda>
-	Count tutte le domande
-	Count domande aperte
-	maxPartecipation = max (answerCount)
-	Sum(answerCount) di tutte le domande
-	Tasso di chiusura = domande aperte/totale domande
-	Partecipazione media = Sum(answerCount)/totale domande
-	Bassa = < maxPartecipation/3
-	Medio = beetween  maxPartecipation/3 and 2(maxPartecipation/3)
-	Alta = > 2(maxPartecipation/3)

Risultato:
-	<tag, tassoDiChiusura, partecipazioneMedia, discretizzazione>

 */
public class ClosingRateWithAverageParticipation implements JobProvider {
    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private final Path outputPath;

    public ClosingRateWithAverageParticipation(Class<?> mainClass, Configuration conf, Path inputPath, Path outputPath) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    @Override
    public Job get() throws IOException {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapOutput.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, inputPath, KeyValueTextInputFormat.class, InputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Finisher.class);

        job.setSortComparatorClass(Text.Comparator.class);

        return job;
    }

    public static final class InputMapper extends Mapper<Text, Text, Text, MapOutput> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            final Question question = Question.create(value);
            context.write(key, MapOutput.create(question));
        }
    }

    public static final class Combiner extends Reducer<Text, MapOutput, Text, MapOutput> {
        @Override
        protected void reduce(Text key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {
            context.write(key, aggregate(values));
        }
    }

    public static final class Finisher extends Reducer<Text, MapOutput, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {
            final MapOutput mapOutput = aggregate(values);

            final double totalQuestions = mapOutput.totalQuestions();
            int openQuestions = mapOutput.openQuestions();
            final double openingRate = openQuestions / totalQuestions;

            int answerCount = mapOutput.answerCount();
            final double averageParticipation = answerCount / totalQuestions;

            context.write(key, new Text(String.format("(%d,%d,%d,%.2f%%,%.2f)", openQuestions, (int) totalQuestions, answerCount, openingRate * 100, averageParticipation)));
        }
    }

    private static MapOutput aggregate(Iterable<? extends MapOutput> values) {
        int openQuestions = 0;
        int totalQuestions = 0;
        int totalAnswers = 0;

        for (MapOutput val : values) {
            openQuestions += val.openQuestions();
            totalQuestions += val.totalQuestions();
            totalAnswers += val.answerCount();
        }

        return MapOutput.create(openQuestions, totalQuestions, totalAnswers);
    }
}
