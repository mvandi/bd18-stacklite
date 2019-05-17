package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.io.Text;

public final class TagScore extends TupleWritable {

    public static String format(String left, int right) {
        return String.format("(%s,%d)", left, right);
    }

    public static TagScore create(String left, int right) {
        return new TagScore(left, right);
    }

    public static TagScore create(Text text) {
        return create(text.toString());
    }

    public static TagScore create(String text) {
        final int lastComma = text.lastIndexOf(",");

        final String left = text.substring(1, lastComma);
        final int right = Integer.parseInt(text.substring(lastComma + 1, text.length() - 1));

        return create(left, right);
    }

    public String tag() {
        return get(0);
    }

    public int score() {
        return get(1);
    }

    public TagScore() {
        super();
    }

    private TagScore(String tag, int score) {
        super(tag, score);
    }

}
