package it.unibo.bd18.util;

import org.apache.commons.lang.ClassUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class TupleWritable implements Writable {

    private final ArrayWritable delegate;

    protected TupleWritable(Object... values) {
        delegate = new ArrayWritable(ObjectWritable.class);

        final ObjectWritable[] result = new ObjectWritable[values.length];

        for (int i = 0; i < values.length; i++) {
            final Object value = values[i];
            if (value instanceof ObjectWritable)
                result[i] = unwrap((ObjectWritable) value);
            else
                result[i] = wrap(value);
        }
        delegate.set(result);
    }

    @Override
    public final void write(DataOutput out) throws IOException {
        delegate.write(out);
    }

    @Override
    public final void readFields(DataInput in) throws IOException {
        delegate.readFields(in);
    }

    protected final <T> T get(int index) {
        return (T) ((ObjectWritable) delegate.get()[index]).get();
    }

    private static ObjectWritable unwrap(ObjectWritable value) {
        while (value.get() instanceof ObjectWritable)
            value = (ObjectWritable) value.get();

        return value;
    }

    private static ObjectWritable wrap(Object value) {
        if (value == null)
            return new ObjectWritable(NullWritable.get());

        final Class<?> primitiveClass = ClassUtils.wrapperToPrimitive(value.getClass());
        return primitiveClass != null
                ? new ObjectWritable(primitiveClass, value)
                : new ObjectWritable(value);
    }

}
