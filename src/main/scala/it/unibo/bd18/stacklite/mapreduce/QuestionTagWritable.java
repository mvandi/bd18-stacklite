package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionTagData;
import it.unibo.bd18.util.WritableWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class QuestionTagWritable extends WritableWrapper<QuestionTagData> {

    public static QuestionTagWritable create(QuestionTagData tag) {
        return new QuestionTagWritable(tag);
    }

    public QuestionTagWritable() {
        super();
    }

    private QuestionTagWritable(QuestionTagData tag) {
        super(tag);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(obj.toCSVString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        obj = QuestionTagData.create(in.readLine());
    }

}
