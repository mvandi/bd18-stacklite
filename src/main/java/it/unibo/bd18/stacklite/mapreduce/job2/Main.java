package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.C;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.CompositeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public final class Main extends Configured implements Tool {

    @Override
    public int run(String... args) throws Exception {
        final Path questionsPath = new Path(C.hdfs.data.questions);
        final Path questionTagsPath = new Path(C.hdfs.data.questionTags);
        final Path tempPath = new Path(args[0] + "-temp");
        final Path resultPath = new Path(args[0]);

        final Configuration conf = getConf();
        final Class mainClass = getClass();

        try (final FileSystem fs = FileSystem.get(conf)) {
            Utils.deleteIfExists(fs, true, tempPath, resultPath);
            try {
                return new CompositeJob()
                        .add(new Join(mainClass, conf, questionsPath, questionTagsPath, tempPath))
                        .add(new OpeningRateWithAverageParticipation(mainClass, conf, tempPath, resultPath))
                        .waitForCompletion(true) ? 0 : 1;
            } finally {
                Utils.deleteIfExists(fs, true, tempPath);
            }
        }
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

}