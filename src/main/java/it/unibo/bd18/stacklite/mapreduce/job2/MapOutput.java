package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.TupleWritable;

public class MapOutput extends TupleWritable {

    public static MapOutput create(Question question) {
        return new MapOutput(isOpen(question), 1, getAnswerCount(question));
    }

    public static MapOutput create(int openQuestions, int totalQuestions, int totalAnswers) {
        return new MapOutput(openQuestions, totalQuestions, totalAnswers);
    }

    public int openQuestions() {
        return get(0);
    }

    public int totalQuestions() {
        return get(1);
    }

    public int answerCount() {
        return get(2);
    }

    public MapOutput() {
        super();
    }

    private MapOutput(int openQuestions, int totalQuestions, int answerCount) {
        super(openQuestions, totalQuestions, answerCount);
    }

    private static int isOpen(Question question) {
        return question.closedDate() == null ? 1 : 0;
    }

    private static int getAnswerCount(Question question) {
        final Integer answerCount = question.answerCount();
        return answerCount == null ? 0 : answerCount;
    }

}
