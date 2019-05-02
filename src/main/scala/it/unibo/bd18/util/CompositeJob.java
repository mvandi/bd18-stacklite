package it.unibo.bd18.util;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
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

    public CompositeJob add(Iterable<? extends JobProvider> providers) {
        checkDefine();
        Validate.notNull(providers, "providers are null");
        return addInternal(providers);
    }

    public CompositeJob add(JobProvider[] providers) {
        checkDefine();
        Validate.notNull(providers, "providers are null");
        return addInternal(providers);
    }

    public boolean isCompleted() {
        checkNotDefine();
        return state == State.SUCCEEDED || state == State.FAILED;
    }

    public boolean isSuccessful() {
        checkCompleted();
        return state == State.SUCCEEDED;
    }

    public boolean waitForCompletion(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        checkDefine();

        state = State.RUNNING;
        final Iterator<JobProvider> it = jobs.iterator();
        try {
            while (it.hasNext()) {
                final Job job = it.next().get();
                Validate.notNull(job, "provided job is null");
                if (!job.waitForCompletion(verbose)) {
                    jobs.clear();
                    state = State.FAILED;
                    return false;
                }
                it.remove();
            }
        } catch (final Exception e) {
            jobs.clear();
            state = State.FAILED;
            throw e;
        }
        state = State.SUCCEEDED;

        return true;
    }

    private void addInternal(JobProvider provider) {
        Validate.notNull(provider, "provider is null");
        jobs.add(provider);
    }

    private CompositeJob addInternal(Iterable<? extends JobProvider> providers) {
        for (final JobProvider provider : providers) {
            addInternal(provider);
        }
        return this;
    }

    private CompositeJob addInternal(JobProvider[] providers) {
        return addInternal(Arrays.asList(providers));
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
