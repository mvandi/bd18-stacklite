package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.util.WritableWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class QuestionWritable extends WritableWrapper<QuestionData> {

    public static QuestionWritable create(QuestionData question) {
        return new QuestionWritable(question);
    }

    public QuestionWritable() {
        super();
    }

    private QuestionWritable(QuestionData question) {
        super(question);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(obj.toCSVString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        obj = QuestionData.create(in.readLine());
    }
}
