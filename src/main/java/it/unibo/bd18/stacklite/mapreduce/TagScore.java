package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public final class TagScore extends TupleWritable {

    public static String format(String left, int right) {
        return String.format("(%s,%d)", left, right);
    }

    public static TagScore create(Text left, IntWritable right) {
        return new TagScore(left, right);
    }

    public static TagScore create(String left, int right) {
        return create(new Text(left), new IntWritable(right));
    }

    public static TagScore create(Text text) {
        return create(text.toString());
    }

    public static TagScore create(String text) {
        final int lastComma = text.lastIndexOf(",");

        final Text left = new Text(text.substring(1, lastComma));
        final IntWritable right = new IntWritable(Integer.parseInt(text.substring(lastComma + 1, text.length() - 1)));

        return create(left, right);
    }

    public String tag() {
        return this.<Text>get(0).toString();
    }

    public int score() {
        return this.<IntWritable>get(1).get();
    }

    public TagScore() {
        super();
    }

    private TagScore(Text tag, IntWritable score) {
        super(tag, score);
    }

}
