package it.unibo.bd18.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TupleWritable implements Writable {

    private final ArrayWritable delegate;

    public TupleWritable(Writable... values) {
        delegate = new ArrayWritable(ObjectWritable.class);
        delegate.set(values);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        delegate.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        delegate.readFields(in);
    }

    public final int length() {
        return delegate.get().length;
    }

    public final <T> T get(int index) {
        return (T) ((ObjectWritable) delegate.get()[index]).get();
    }

    public final Object[] get() {
        final Object[] result = new Object[length()];
        for (int i = 0; i < result.length; i++) {
            result[i] = this.get(i);
        }
        return result;
    }

    protected final void set(Writable... values) {
        for (int i = 0; i < values.length; i++) {
            final Writable value = values[i];
            if (!(value instanceof ObjectWritable))
                values[i] = new ObjectWritable(value);
        }
        delegate.set(values);
    }

}
