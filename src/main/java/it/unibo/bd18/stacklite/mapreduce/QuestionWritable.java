package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.Question;
import it.unibo.bd18.util.WritableWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class QuestionWritable extends WritableWrapper<Question> {

    public static QuestionWritable create(Question question) {
        return new QuestionWritable(question);
    }

    public QuestionWritable() {
        super();
    }

    private QuestionWritable(Question question) {
        super(question);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(obj.toCSVString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        obj = Question.create(in.readLine());
    }
}
