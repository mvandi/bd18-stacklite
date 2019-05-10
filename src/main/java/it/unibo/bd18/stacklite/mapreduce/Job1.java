package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.CompositeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Find the first five tags that received the highest sum of scores for each
 * year-month pair (tags sorted in descending order).
 */
public final class Job1 extends Configured implements Tool {

    @Override
    public int run(String... args) throws Exception {
        final Path questionsPath = new Path(args[0]);
        final Path questionTagsPath = new Path(args[1]);
        final Path tempPath = new Path(args[2] + "-temp");
        final Path resultPath = new Path(args[2]);

        final Configuration conf = getConf();
        final Class mainClass = getClass();

        try (final FileSystem fs = FileSystem.get(conf)) {
            Utils.deleteIfExists(fs, true, tempPath, resultPath);

            try {
                return new CompositeJob()
                        .add(Join.create(mainClass, conf, questionsPath, questionTagsPath, tempPath))
                        .add(HighestScoreTags.create(mainClass, conf, tempPath, resultPath))
                        .waitForCompletion(true) ? 0 : 1;
            } finally {
                Utils.deleteIfExists(fs, true, tempPath);
            }
        }
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Job1(), args));
    }

}
