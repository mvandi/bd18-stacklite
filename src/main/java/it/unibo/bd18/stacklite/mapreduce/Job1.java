package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public final class Job1 extends Configured implements Tool {

    public static class QuestionMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String row = value.toString();
            if (!Utils.isHeader(row)) {
                throw new UnsupportedOperationException("QuestionMapper not implemented");
            }
        }
    }

    public static class QuestionTagMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            final String row = value.toString();
            if (!Utils.isHeader(row)) {
                throw new UnsupportedOperationException("QuestionTagMapper not implemented");
            }
        }
    }

    public static class Join extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            throw new UnsupportedOperationException("Join not implemented");
        }
    }

    @Override
    public int run(String... args) throws Exception {
        final Path questionsFile = new Path(args[0]);
        final Path questionTagsFile = new Path(args[1]);
        final Path resultPath = new Path(args[2]);

        final Configuration conf = getConf();

        final FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(resultPath)) {
            fs.delete(resultPath, true);
        }

        final Job job = Job.getInstance(conf, "Job1");

        job.setJarByClass(getClass());

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, questionsFile, FileInputFormat.class, QuestionMapper.class);
        MultipleInputs.addInputPath(job, questionTagsFile, FileInputFormat.class, QuestionTagMapper.class);
        FileOutputFormat.setOutputPath(job, resultPath);

        job.setCombinerClass(Join.class);
        job.setReducerClass(Join.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Job1(), args));
    }

}
