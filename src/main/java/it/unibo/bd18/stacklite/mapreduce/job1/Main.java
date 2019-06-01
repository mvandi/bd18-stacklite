package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.CompositeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static it.unibo.bd18.stacklite.C.hdfs;

/**
 * Find the first five tags that received the highest sum of scores for each
 * year-month pair (tags sorted in descending order).
 */
public final class Main extends Configured implements Tool {

    @Override
    public int run(String... args) throws Exception {
        final Path questionsPath = new Path(hdfs.data.questions);
        final Path questionTagsPath = new Path(hdfs.data.questionTags);
        final String resultPathStr = args[0];
        final Path joinPath = new Path(resultPathStr + "-join");
        final Path resultPath = new Path(resultPathStr);

        final Configuration conf = getConf();
        final Class mainClass = getClass();

        try (final FileSystem fs = FileSystem.get(conf)) {
            Utils.deleteIfExists(fs, true, resultPath, joinPath);

            try {
                return new CompositeJob()
                        .add(new Join(mainClass, conf, questionsPath, questionTagsPath, joinPath))
                        .add(new HighestScoreTags(mainClass, conf, joinPath, resultPath))
                        .waitForCompletion(true) ? 0 : 1;
            } finally {
                Utils.deleteIfExists(fs, true, joinPath);
            }
        }
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

}
