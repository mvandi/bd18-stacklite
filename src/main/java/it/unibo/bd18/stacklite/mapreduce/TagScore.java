package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.util.PairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TagScore extends PairWritable<Text, IntWritable> {

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
        return get().left().toString();
    }

    public int score() {
        return get().right().get();
    }

    public TagScore() {
        super();
    }

    private TagScore(Text left, IntWritable right) {
        super(left, right);
    }

    @Override
    protected Text createLeft() {
        return new Text();
    }

    @Override
    protected IntWritable createRight() {
        return new IntWritable();
    }

}
