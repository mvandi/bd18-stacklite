package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.io.Text;

public final class ReduceOutputKey extends TupleWritable implements Comparable<ReduceOutputKey> {

    public static ReduceOutputKey create(Text text) {
        return create(text.toString());
    }

    public static ReduceOutputKey create(String text) {
        final String[] values = text.substring(1, text.length() - 1).split(",");
        return create(Integer.parseInt(values[0]), Integer.parseInt(values[1]));
    }

    public static ReduceOutputKey create(int year, int month) {
        return new ReduceOutputKey(year, month);
    }

    public int year() {
        return get(0);
    }

    public int month() {
        return get(1);
    }

    @Override
    public int compareTo(ReduceOutputKey o) {
        final int yc = year() - o.year();
        return yc == 0 ? month() - o.month() : yc;
    }

    public ReduceOutputKey() {
    }

    private ReduceOutputKey(int year, int month) {
        super(year, month);
    }

}
