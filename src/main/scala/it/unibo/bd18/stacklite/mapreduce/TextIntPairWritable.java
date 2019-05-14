package it.unibo.bd18.stacklite.mapreduce;

import it.unibo.bd18.util.PairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TextIntPairWritable extends PairWritable<Text, IntWritable> {

    public static TextIntPairWritable create(Text left, IntWritable right) {
        return new TextIntPairWritable(left, right);
    }

    public static String format(String left, int right) {
        return String.format("(%s,%d)", left, right);
    }

    public static TextIntPairWritable create(Text text) {
        return create(text.toString());
    }

    public static TextIntPairWritable create(String text) {
        final int lastComma = text.lastIndexOf(",");

        final Text left = new Text(text.substring(1, lastComma));
        final IntWritable right = new IntWritable(Integer.parseInt(text.substring(lastComma + 1, text.length() - 1)));

        return create(left, right);
    }

    public TextIntPairWritable() {
        super();
    }

    private TextIntPairWritable(Text left, IntWritable right) {
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
