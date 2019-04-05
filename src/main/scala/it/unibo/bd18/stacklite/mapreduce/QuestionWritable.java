package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.stacklite.QuestionData;
import it.unibo.bd18.stacklite.Utils;
import it.unibo.bd18.util.WritableWrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class QuestionWritable extends WritableWrapper<QuestionData> {

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
        out.write(obj.id());
        out.writeUTF(Utils.toString(obj.creationDate()));
        out.writeUTF(Utils.toString(obj.closedDate()));
        out.writeUTF(Utils.toString(obj.deletionDate()));
        out.write(obj.score());
        out.writeUTF(Utils.toString(obj.ownerUserId()));
        out.writeUTF(Utils.toString(obj.answerCount()));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        obj = new QuestionData(
                in.readInt(),
                Utils.readDate(in.readUTF()),
                Utils.readDate(in.readUTF()),
                Utils.readDate(in.readUTF()),
                in.readInt(),
                Utils.readIntBoxed(in.readUTF()),
                Utils.readIntBoxed(in.readUTF()));
    }
}
