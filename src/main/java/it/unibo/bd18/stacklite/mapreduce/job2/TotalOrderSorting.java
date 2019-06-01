package it.unibo.bd18.stacklite.mapreduce.job2;

import it.unibo.bd18.stacklite.mapreduce.AbstractTotalOrderSorting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.io.Text.Comparator;

public final class TotalOrderSorting extends AbstractTotalOrderSorting {

    public TotalOrderSorting(Class<?> mainClass, Configuration conf, Path inputPath, Path partitionFile, Path outputPath) {
        super(mainClass, conf, inputPath, partitionFile, outputPath);
    }

    @Override
    protected Class<Comparator> getComparatorClass() {
        return Comparator.class;
    }

}
