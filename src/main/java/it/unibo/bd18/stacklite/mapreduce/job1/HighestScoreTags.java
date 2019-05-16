package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.stacklite.mapreduce.TagScore;
import it.unibo.bd18.util.JobProvider;
import it.unibo.bd18.util.Pair;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public final class HighestScoreTags implements JobProvider {

    private final Class<?> mainClass;
    private final Configuration conf;
    private final Path inputPath;
    private final Path outputPath;

    public HighestScoreTags(Class<?> mainClass, Configuration conf, Path inputPath, Path outputPath) {
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
        job.setMapOutputValueClass(TagScore.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, inputPath, KeyValueTextInputFormat.class, InputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Finisher.class);

        return job;
    }

    public static final class InputMapper extends Mapper<Text, Text, Text, TagScore> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, TagScore.create(value));
        }
    }

    public static final class Combiner extends Reducer<Text, TagScore, Text, TagScore> {
        @Override
        protected void reduce(Text key, Iterable<TagScore> values, Context context) throws IOException, InterruptedException {
            final Map<String, Integer> tags = sumScoresByTag(values);
            for (final Entry<String, Integer> e : tags.entrySet()) {
                final TagScore valueOut = TagScore.create(e.getKey(), e.getValue());
                context.write(key, valueOut);
            }
        }
    }

    public static final class Finisher extends Reducer<Text, TagScore, Text, Text> {
        private final Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<TagScore> values, Context context) throws IOException, InterruptedException {
            final Map<String, Integer> tags = sumScoresByTag(values);
//            final List<String> result = Utils.sortedKeysByValue(tags, false).subList(0, 5);
            List<Pair<String, Integer>> result = Utils.sortedByValue(tags, false).subList(0, 5);
            valueOut.set(result.toString());
            context.write(key, valueOut);
        }
    }

    private static Map<String, Integer> sumScoresByTag(Iterable<? extends TagScore> values) {
        final Map<String, Integer> tags = new HashMap<>();
        for (final TagScore value : values) {
            final String tag = value.tag();
            final int score = value.score();
            final Integer oldScore = tags.get(tag);
            if (oldScore != null) {
                tags.put(tag, oldScore + score);
            } else {
                tags.put(tag, score);
            }
        }
        return tags;
    }

}
