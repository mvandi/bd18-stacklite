package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.util.JobProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class MinMax implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;

    public MinMax(Class mainClass, Configuration conf, Path inputPath) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.inputPath = inputPath;
    }

    @Override
    public Job get() throws Exception {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MinMaxOutputValue.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Object.class);

        MultipleInputs.addInputPath(job, inputPath, KeyValueTextInputFormat.class, InputMapper.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Finisher.class);

        return job;
    }

    public static final class InputMapper extends Mapper<Text, Text, IntWritable, MinMaxOutputValue> {

        private final IntWritable keyOut = new IntWritable(0);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double averageParticipation = Double.parseDouble(value.toString());
            context.write(keyOut, MinMaxOutputValue.create(averageParticipation, averageParticipation));
        }
    }

    public static final class Combiner extends Reducer<IntWritable, MinMaxOutputValue, IntWritable, MinMaxOutputValue> {
        @Override
        protected void reduce(IntWritable key, Iterable<MinMaxOutputValue> values, Context context) throws IOException, InterruptedException {
            context.write(key, minmax(values));
        }
    }

    public static final class Finisher extends Reducer<IntWritable, MinMaxOutputValue, IntWritable, Object> {
        @Override
        protected void reduce(IntWritable key, Iterable<MinMaxOutputValue> values, Context context) throws IOException, InterruptedException {
            final MinMaxOutputValue result = minmax(values);

            final Properties props = new Properties();
            props.setProperty("min", Double.toString(result.min()));
            props.setProperty("max", Double.toString(result.max()));

            final Configuration conf = context.getConfiguration();
            final String outputPath = conf.get("minmax.properties");
            final FileSystem fs = FileSystem.get(conf);
            final OutputStream os = fs.create(new Path(outputPath));

            props.store(os, null);
        }
    }

    private static MinMaxOutputValue minmax(Iterable<? extends MinMaxOutputValue> values) {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        for (MinMaxOutputValue value : values) {
            min = Math.min(min, value.min());
            max = Math.max(max, value.max());
        }

        return MinMaxOutputValue.create(min, max);
    }

}
