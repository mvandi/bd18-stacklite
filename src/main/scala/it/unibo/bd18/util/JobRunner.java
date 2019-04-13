package it.unibo.bd18.util;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class JobRunner {

    public static boolean runAll(boolean verbose, List<? extends Job> jobs) throws InterruptedException, IOException, ClassNotFoundException {
        for (final Job job : jobs) {
            if (!job.waitForCompletion(verbose)) {
                return false;
            }
        }
        return true;
    }

    public static boolean runAll(boolean verbose, Job... jobs) throws InterruptedException, IOException, ClassNotFoundException {
        return runAll(verbose, Arrays.asList(jobs));
    }

}
