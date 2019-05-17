package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.io.Text;

public final class MapOutputValue extends TupleWritable {

    public static String format(String left, int right) {
        return String.format("(%s,%d)", left, right);
    }

    public static MapOutputValue create(String left, int right) {
        return new MapOutputValue(left, right);
    }

    public static MapOutputValue create(Text text) {
        return create(text.toString());
    }

    public static MapOutputValue create(String text) {
        final int lastComma = text.lastIndexOf(",");

        final String tag = text.substring(1, lastComma);
        final int score = Integer.parseInt(text.substring(lastComma + 1, text.length() - 1));

        return create(tag, score);
    }

    public String tag() {
        return get(0);
    }

    public int score() {
        return get(1);
    }

    public MapOutputValue() {
        super();
    }

    private MapOutputValue(String tag, int score) {
        super(tag, score);
    }

}
