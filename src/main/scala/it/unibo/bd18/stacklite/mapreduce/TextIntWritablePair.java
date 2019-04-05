package it.unibo.bd18.stacklite.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TextIntWritablePair extends PairWritable<Text, IntWritable> {

    public static TextIntWritablePair create(Text left, IntWritable right) {
        return new TextIntWritablePair(left, right);
    }

    public TextIntWritablePair() {
        super();
    }

    private TextIntWritablePair(Text left, IntWritable right) {
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
