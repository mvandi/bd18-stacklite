package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.TupleWritable;

public class MapOutputValue extends TupleWritable {

    public static MapOutputValue create(Question question) {
        return new MapOutputValue(open(question), 1, answerCount(question));
    }

    public static MapOutputValue create(int openQuestions, int questionCount, int totalAnswers) {
        return new MapOutputValue(openQuestions, questionCount, totalAnswers);
    }

    public int openQuestions() {
        return get(0);
    }

    public int questionCount() {
        return get(1);
    }

    public int totalAnswers() {
        return get(2);
    }

    public MapOutputValue() {
        super();
    }

    private MapOutputValue(int openQuestions, int questionCount, int totalAnswers) {
        super(openQuestions, questionCount, totalAnswers);
    }

    private static int open(Question question) {
        return question.closedDate() == null ? 1 : 0;
    }

    private static int answerCount(Question question) {
        final Integer answerCount = question.answerCount();
        return answerCount == null ? 0 : answerCount;
    }

}
