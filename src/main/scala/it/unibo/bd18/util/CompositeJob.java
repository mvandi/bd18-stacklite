package it.unibo.bd18.util;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class CompositeJob {

    public static Builder add(Iterable<? extends Job> jobs) {
        return new Builder().add(jobs);
    }

    public static Builder add(Job first, Job... more) {
        return new Builder().add(first, more);
    }

    public static class Builder {

        private boolean running = false;

        private final List<Job> jobs;

        private Builder() {
            jobs = new LinkedList<>();
        }

        public Builder add(Iterable<? extends Job> jobs) {
            checkNotRunning();
            if (jobs != null) {
                for (final Job job : jobs) {
                    addInternal(job);
                }
            }
            return this;
        }

        public Builder add(Job first, Job... more) {
            checkNotRunning();
            addInternal(first);
            return add(Arrays.asList(more));
        }

        public boolean waitForCompletion(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
            checkNotRunning();
            running = true;
            final Iterator<Job> it = jobs.iterator();
            while (it.hasNext()) {
                if (!it.next().waitForCompletion(verbose)) {
                    while (it.hasNext())
                        it.remove();
                    return false;
                }
                it.remove();
            }
            running = false;
            return true;
        }

        private void addInternal(Job job) {
            requireNonNull(job, "job is null");
            jobs.add(job);
        }

        private void checkNotRunning() {
            if (running) {
                throw new IllegalStateException("adder is running");
            }
        }

    }

}
