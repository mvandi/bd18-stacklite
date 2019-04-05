package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.util.Pair;
import it.unibo.bd18.util.WritableWrapper;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class PairWritable<L extends Writable, R extends Writable> extends WritableWrapper<Pair<L, R>> {

    protected PairWritable() {
        super();
    }

    protected PairWritable(L left, R right) {
        super(Pair.create(left, right));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        obj.left().write(out);
        obj.right().write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        final L left = createLeft();
        final R right = createRight();

        left.readFields(in);
        right.readFields(in);

        obj = Pair.create(left, right);
    }

    protected abstract L createLeft();

    protected abstract R createRight();

}
