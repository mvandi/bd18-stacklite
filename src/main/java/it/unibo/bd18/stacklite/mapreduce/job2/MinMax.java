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

import java.io.IOException;

public class MinMax implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private MutableInt min;
    private MutableInt max;

    public MinMax(Class mainClass, Configuration conf, Path inputPath, MutableInt min, MutableInt max) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.inputPath = inputPath;
        this.min = min;
        this.max = max;
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

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Finisher.class);

        return job;
    }

    public static final class InputMapper extends Mapper<Text, Text, IntWritable, IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            final Question question = Question.create(value);
            context.write(new IntWritable(0), new IntWritable(MapOutputValue.create(question).totalAnswers()));
        }
    }

    public final class Combiner extends Reducer<IntWritable, IntWritable, IntWritable, MinMaxOutputValue> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, minmax(values));
        }
    }

    public final class Finisher extends Reducer<IntWritable, MinMaxOutputValue, IntWritable, MinMaxOutputValue> {
        @Override
        protected void reduce(IntWritable key, Iterable<MinMaxOutputValue> values, Context context) throws IOException, InterruptedException {
            int min = 0;
            int max = 0;

            for (MinMaxOutputValue value : values) {
                min = min(value.min().intValue(), min);
                max = max(value.max().intValue(), max);
            }

            setMinMax(min, max);
        }
    }

    private void setMinMax(int min, int max) {
        this.min.setValue(min);
        this.max.setValue(max);
    }

    private int min(int i, int j) {
        return (i < j) ? i : j;
    }

    private int max(int i, int j) {
        return (i > j) ? i : j;
    }

    private MinMaxOutputValue minmax(Iterable<IntWritable> values) {
        int min = 0;
        int max = 0;

        for (IntWritable value : values) {
            min = min(value.get(), min);
            max = max(value.get(), max);
        }

        this.setMinMax(min, max);

        return MinMaxOutputValue.create(this.min, this.max);
    }
}
