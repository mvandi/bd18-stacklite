package it.unibo.bd18.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class CompositeJob {

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

    public CompositeJob add(Iterable<? extends JobProvider> jobs) {
        checkDefine();
        requireNonNull(jobs, "jobs are null");
        return addInternal(jobs);
    }

    public CompositeJob add(JobProvider[] jobs) {
        checkDefine();
        requireNonNull(jobs, "jobs are null");
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

    public boolean waitForCompletion(boolean verbose) throws InterruptedException, IOException, ClassNotFoundException {
        checkDefine();

        state = State.RUNNING;
        final Iterator<JobProvider> it = jobs.iterator();
        while (it.hasNext()) {
            if (!it.next().get().waitForCompletion(verbose)) {
                jobs.clear();
                state = State.FAILED;
                return false;
            }
            it.remove();
        }
        state = State.SUCCEEDED;

        return true;
    }

    private void addInternal(JobProvider job) {
        requireNonNull(job, "job is null");
        jobs.add(job);
    }

    private CompositeJob addInternal(Iterable<? extends JobProvider> jobs) {
        for (final JobProvider job : jobs) {
            addInternal(job);
        }
        return this;
    }

    private CompositeJob addInternal(JobProvider[] jobs) {
        return addInternal(Arrays.asList(jobs));
    }

    private void checkDefine() {
        checkExpression(state == State.DEFINE, "jobs are either already running or already completed");
    }

    private void checkNotDefine() {
        checkExpression(state != State.DEFINE, "jobs are still being defined");
    }

    private void checkCompleted() {
        checkExpression(state == State.SUCCEEDED || state == State.FAILED, "jobs are not completed");
    }

    private static void checkExpression(boolean expression, String message, Object... args) {
        if (!expression) {
            throw new IllegalStateException(String.format(message, args));
        }
    }

}
