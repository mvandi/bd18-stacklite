package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.mapreduce.AbstractTotalOrderSorting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

public final class TotalOrderSorting extends AbstractTotalOrderSorting {

    public TotalOrderSorting(Class<?> mainClass, Configuration conf, Path inputPath, Path partitionFile, Path outputPath) {
        super(mainClass, conf, inputPath, partitionFile, outputPath);
    }

    @Override
    protected Class<? extends RawComparator> getComparatorClass() {
        return Text.Comparator.class;
    }

}
