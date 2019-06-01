package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.stacklite.mapreduce.job2.C.minmax;
import it.unibo.bd18.util.CompositeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static it.unibo.bd18.stacklite.C.hdfs;

public final class Main extends Configured implements Tool {

    @Override
    public int run(String... args) throws Exception {
        final Path questionsPath = new Path(hdfs.data.questions);
        final Path questionTagsPath = new Path(hdfs.data.questionTags);
        final String resultPathStr = args[0];
        final Path joinPath = new Path(resultPathStr + "-join");
        final Path averageParticipationByTagPath = new Path(resultPathStr + "-temp");
        final String minmaxPathStr = resultPathStr + "-minmax.properties";
        final Path minmaxPath = new Path(minmaxPathStr);
        final Path resultPath = new Path(resultPathStr);

        final Configuration conf = getConf();
        final Class mainClass = getClass();

        try (final FileSystem fs = FileSystem.get(conf)) {
            Utils.deleteIfExists(fs, true, resultPath, joinPath, averageParticipationByTagPath, minmaxPath);
            try {
                conf.set(minmax.properties.path, minmaxPathStr);
                return new CompositeJob()
                        .add(new Join(mainClass, conf, questionsPath, questionTagsPath, joinPath))
                        .add(new AverageParticipationByTag(mainClass, conf, joinPath, averageParticipationByTagPath))
                        .add(new MinMax(mainClass, conf, averageParticipationByTagPath))
                        .add(new OpeningRateWithParticipation(mainClass, conf, joinPath, resultPath))
                        .waitForCompletion(true) ? 0 : 1;
            } finally {
                Utils.deleteIfExists(fs, true, joinPath, averageParticipationByTagPath, minmaxPath);
            }
        }
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

}
