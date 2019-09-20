package it.unibo.bd18.util;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.mapreduce.Job;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class CompositeJob {

    private final List<JobProvider> jobs;

    private enum State {
        DEFINE,
        RUNNING,
        SUCCEEDED,
        FAILED
    }
    private State state;

    public CompositeJob() {
        jobs = new LinkedList<>();
        state = State.DEFINE;
    }

    public CompositeJob add(JobProvider first, JobProvider... more) {
        checkDefine();
        addInternal(first);
        return addInternal(more);
    }

    public CompositeJob add(List<? extends JobProvider> jobs) {
        checkDefine();
        Validate.notNull(jobs, "jobs are null");
        return addInternal(jobs);
    }

    public CompositeJob add(JobProvider[] jobs) {
        checkDefine();
        Validate.notNull(jobs, "jobs are null");
        return addInternal(jobs);
    }

    public boolean isCompleted() {
        checkNotDefine();
        return state == State.SUCCEEDED || state == State.FAILED;
    }

    public boolean isSuccessful() {
        checkCompleted();
        return state == State.SUCCEEDED;
    }

    public boolean waitForCompletion(boolean verbose) throws Exception {
        checkDefine();

        state = State.RUNNING;
        final Iterator<JobProvider> it = jobs.iterator();
        try {
            while (it.hasNext()) {
                final Job job = it.next().get();
                Validate.notNull(job, "provided job is null");
                if (!job.waitForCompletion(verbose)) {
                    cleanUpFailure();
                    return false;
                }
                it.remove();
            }
        } catch (final Exception e) {
            cleanUpFailure();
            throw e;
        }
        state = State.SUCCEEDED;

        return true;
    }

    private void cleanUpFailure() {
        jobs.clear();
        state = State.FAILED;
    }

    private void addInternal(JobProvider job) {
        Validate.notNull(job, "job is null");
        this.jobs.add(job);
    }

    private CompositeJob addInternal(List<? extends JobProvider> jobs) {
        for (final JobProvider provider : jobs) {
            addInternal(provider);
        }
        return this;
    }

    private CompositeJob addInternal(JobProvider[] jobs) {
        for (final JobProvider provider : jobs) {
            addInternal(provider);
        }
        return this;
    }

    private void checkDefine() {
        Validate.isTrue(state == State.DEFINE, "jobs are either already running or already completed");
    }

    private void checkNotDefine() {
        Validate.isTrue(state != State.DEFINE, "jobs are still being defined");
    }

    private void checkCompleted() {
        Validate.isTrue(state == State.SUCCEEDED || state == State.FAILED, "jobs are not completed");
    }

}
