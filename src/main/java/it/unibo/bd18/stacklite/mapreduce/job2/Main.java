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

        final Path resultPath = new Path(args[0]);

        final Path joinPath = new Path(hdfs.cache.job2.join);
        final Path averageParticipationByTagPath = new Path(hdfs.cache.job2.averageParticipationByTag);
        final String minmaxPathStr = hdfs.cache.job2.minmax;
        final Path minmaxPath = new Path(minmaxPathStr);

        final Configuration conf = getConf();
        final Class mainClass = getClass();

        try (final FileSystem fs = FileSystem.get(conf)) {
            Utils.deleteIfExists(fs, true, resultPath);
            conf.set(minmax.properties.path, minmaxPathStr);

            final CompositeJob cj = new CompositeJob();

            if (!fs.exists(joinPath))
                cj.add(new Join(mainClass, conf, questionsPath, questionTagsPath, joinPath));
            if (!fs.exists(averageParticipationByTagPath))
                cj.add(new AverageParticipationByTag(mainClass, conf, joinPath, averageParticipationByTagPath));
            if (!fs.exists(minmaxPath))
                cj.add(new MinMax(mainClass, conf, averageParticipationByTagPath));

            return cj
                    .add(new OpeningRateWithParticipation(mainClass, conf, joinPath, resultPath))
                    .waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

}
