package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.util.TupleWritable;
import org.apache.commons.lang.mutable.MutableInt;

public class MinMaxOutputValue extends TupleWritable {

    public static MinMaxOutputValue create(MutableInt min, MutableInt max) {
        return new MinMaxOutputValue(min, max);
    }

    public static MinMaxOutputValue create(int min, int max) {
        return new MinMaxOutputValue(new MutableInt(min), new MutableInt(max));
    }

    public MutableInt min() {
        return get(0);
    }

    public MutableInt max() {
        return get(1);
    }

    public MinMaxOutputValue() {
        super();
    }

    private MinMaxOutputValue(MutableInt min, MutableInt max) {
        super(min, max);
    }

}
