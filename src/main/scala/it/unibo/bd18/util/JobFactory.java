package it.unibo.bd18.util;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface JobFactory {

    Job create() throws IOException;

}
