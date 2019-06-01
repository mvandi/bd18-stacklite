package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;

final  class Utils {

    static int open(Question question) {
        return question.closedDate() == null ? 1 : 0;
    }

    static int answerCount(Question question) {
        final Integer answerCount = question.answerCount();
        return answerCount == null ? 0 : answerCount;
    }

    private Utils() {
    }

}
