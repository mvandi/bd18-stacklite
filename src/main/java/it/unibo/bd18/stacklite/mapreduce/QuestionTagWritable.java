package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionTag;
import it.unibo.bd18.util.WritableWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class QuestionTagWritable extends WritableWrapper<QuestionTag> {

    public static QuestionTagWritable create(QuestionTag tag) {
        return new QuestionTagWritable(tag);
    }

    public QuestionTagWritable() {
        super();
    }

    private QuestionTagWritable(QuestionTag tag) {
        super(tag);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(obj.toCSVString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        obj = QuestionTag.create(in.readLine());
    }

}
