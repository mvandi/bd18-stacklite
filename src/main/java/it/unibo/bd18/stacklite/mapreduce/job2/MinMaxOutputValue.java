package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.util.TupleWritable;
import org.apache.hadoop.io.Text;

public class MinMaxOutputValue extends TupleWritable {

    public static String format(double min, double max) {
        return String.format("(%f,%f)", min, max);
    }

    public static MinMaxOutputValue create(double min, double max) {
        return new MinMaxOutputValue(min, max);
    }

    public static MinMaxOutputValue create(Text text) {return create(text.toString());}

    public static MinMaxOutputValue create(String text) {
        final int lastComma = text.lastIndexOf(",");

        final double min = Double.parseDouble(text.substring(1, lastComma));
        final double max = Double.parseDouble(text.substring(lastComma + 1, text.length() - 1));

        return create(min, max);
    }

    public double min() {
        return get(0);
    }

    public double max() {
        return get(1);
    }

    public MinMaxOutputValue() {
        super();
    }

    private MinMaxOutputValue(double min, double max) {
        super(min, max);
    }

}
