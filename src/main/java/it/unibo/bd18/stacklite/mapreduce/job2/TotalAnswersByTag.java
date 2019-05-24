package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.JobProvider;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TotalAnswersByTag implements JobProvider {
    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private final Path outputPath;


    public TotalAnswersByTag(Class mainClass, Configuration conf, Path tempPath, Path outputPath) {
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
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, inputPath, KeyValueTextInputFormat.class, InputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setCombinerClass(Finisher.class);
        job.setReducerClass(Finisher.class);

        return job;
    }

    public static final class InputMapper extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            final Question question = Question.create(value);
            context.write(key, new IntWritable(MapOutputValue.create(question).totalAnswers()));
        }
    }

    public static final class Finisher extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, sum(values));
        }
    }

    private static IntWritable sum(Iterable<? extends IntWritable> values) {
        int totalAnswers = 0;

        for (final IntWritable value : values) {
            totalAnswers += value.get();
        }

        return new IntWritable(totalAnswers);
    }
}
