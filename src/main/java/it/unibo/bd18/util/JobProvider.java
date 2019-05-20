package it.unibo.bd18.util;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface JobProvider {

    Job get() throws IOException, ClassNotFoundException, InterruptedException;

}
