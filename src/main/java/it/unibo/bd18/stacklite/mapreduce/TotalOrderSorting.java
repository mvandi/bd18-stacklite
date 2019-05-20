package it.unibo.bd18.stacklite.mapreduce;

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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class TotalOrderSorting implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private final Path partitionFile;
    private final Path outputPath;

    public TotalOrderSorting(Class<?> mainClass, Configuration conf, Path inputPath, Path partitionFile, Path outputPath) {
        this.mainClass = mainClass;
        this.conf = conf;
        this.inputPath = inputPath;
        this.partitionFile = partitionFile;
        this.outputPath = outputPath;
    }

    @Override
    public Job get() throws IOException, ClassNotFoundException, InterruptedException {
        final Job job = Job.getInstance(conf);

        job.setJarByClass(mainClass);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, inputPath, KeyValueTextInputFormat.class, Mapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setReducerClass(Reducer.class);

        TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
        InputSampler.Sampler<Text, Text> inputSampler = new InputSampler.RandomSampler<>(.01, 1000, 100);
        InputSampler.writePartitionFile(job, inputSampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);

        return job;
    }
}
