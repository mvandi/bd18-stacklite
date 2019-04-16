package it.unibo.bd18.util;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class CompositeJob {

    private volatile boolean running = false;
    private final List<Job> jobs;

    public CompositeJob() {
        jobs = new LinkedList<>();
    }

    public CompositeJob add(Job first, Job... more) {
        checkNotRunning();
        addInternal(first);
        return addInternal(more);
    }

    public CompositeJob add(Iterable<? extends Job> jobs) {
        checkNotRunning();
        requireNonNull(jobs, "jobs are null");
        return addInternal(jobs);
    }

    public CompositeJob add(Job[] jobs) {
        checkNotRunning();
        requireNonNull(jobs, "jobs are null");
        return addInternal(jobs);
    }

    public boolean waitForCompletion(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        checkNotRunning();

        running = true;
        final Iterator<Job> it = jobs.iterator();
        while (it.hasNext()) {
            if (!it.next().waitForCompletion(verbose)) {
                jobs.clear();
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

    private CompositeJob addInternal(Iterable<? extends Job> jobs) {
        for (final Job job : jobs) {
            addInternal(job);
        }
        return this;
    }

    private CompositeJob addInternal(Job[] jobs) {
        return addInternal(Arrays.asList(jobs));
    }

    private void checkNotRunning() {
        if (running) {
            throw new IllegalStateException("jobs are already running");
        }
    }

}
