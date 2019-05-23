package it.unibo.bd18.stacklite.mapreduce.job1;

import it.unibo.bd18.stacklite.mapreduce.AbstractTotalOrderSorting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public final class TotalOrderSorting extends AbstractTotalOrderSorting {

    public TotalOrderSorting(Class<?> mainClass, Configuration conf, Path inputPath, Path partitionFile, Path outputPath) {
        super(mainClass, conf, inputPath, partitionFile, outputPath);
    }

    @Override
    protected Class<Comparator> getComparatorClass() {
        return Comparator.class;
    }

    public static final class Comparator extends WritableComparator {
        protected Comparator() {
            super(Text.class);
        }

        @Override
        public int compare(WritableComparable c1, WritableComparable c2) {
            final ReduceOutputKey a = ReduceOutputKey.create((Text) c1);
            final ReduceOutputKey b = ReduceOutputKey.create((Text) c2);

            return a.compareTo(b);
        }
    }

}
