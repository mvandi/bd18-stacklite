package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.JobFactory;
import it.unibo.bd18.util.Pair;
import it.unibo.bd18.util.PairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class HighestScoreTags {

    public static JobFactory create(final Class<?> mainClass, final Configuration conf, final Path inputPath, final Path outputPath) {
        return new JobFactory() {
            @Override
            public Job create() throws IOException {
                final Job job = Job.getInstance(conf);

                job.setJarByClass(mainClass);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(TextIntPairWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                KeyValueTextInputFormat.addInputPath(job, inputPath);
                FileOutputFormat.setOutputPath(job, outputPath);

                job.setMapperClass(InputMapper.class);
                job.setReducerClass(Finisher.class);

                return job;
            }
        };
    }

    public static final class InputMapper extends Mapper<Text, Text, Text, TextIntPairWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, TextIntPairWritable.create(value));
        }
    }

    public static final class Finisher extends Reducer<Text, TextIntPairWritable, Text, Text> {
        private final Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<TextIntPairWritable> values, Context context) throws IOException, InterruptedException {
            final Map<String, Integer> tags = new HashMap<>();
            for (final PairWritable<Text, IntWritable> value : values) {
                final Pair<Text, IntWritable> t = value.get();
                final String tag = t.left().toString();
                final int score = t.right().get();
                if (tags.containsKey(tag)) {
                    tags.put(tag, tags.get(tag) + score);
                } else {
                    tags.put(tag, score);
                }
            }
            final List<Pair<String, Integer>> result = Utils.sortedByValues(tags, false).subList(0, 5);
            valueOut.set(result.toString());
            context.write(key, valueOut);
        }
    }

    private HighestScoreTags() {
    }

}
