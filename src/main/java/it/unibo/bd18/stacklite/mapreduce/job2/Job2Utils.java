package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;

final class Job2Utils {

    static int open(Question question) {
        return question.closedDate() == null ? 0 : 1;
    }

    static int answerCount(Question question) {
        final Integer answerCount = question.answerCount();
        return answerCount == null ? 0 : answerCount;
    }

    private Job2Utils() {
    }

}
