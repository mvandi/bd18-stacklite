package it.unibo.bd18.util;

import org.apache.hadoop.io.Writable;

import java.util.Objects;

public abstract class WritableWrapper<T> implements Writable {

    protected T obj;

    protected WritableWrapper() {
        this(null);
    }

    protected WritableWrapper(T t) {
        obj = t;
    }

    public T get() {
        Objects.requireNonNull(obj, "obj is null");
        return obj;
    }

}
