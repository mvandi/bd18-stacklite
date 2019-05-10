package it.unibo.bd18.util;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.io.Writable;

public abstract class WritableWrapper<T> implements Writable {

    protected T obj;

    protected WritableWrapper() {
        this(null);
    }

    protected WritableWrapper(T t) {
        obj = t;
    }

    public T get() {
        Validate.notNull(obj, "obj is null");
        return obj;
    }

}
