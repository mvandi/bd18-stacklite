package it.unibo.bd18.util;

import org.apache.hadoop.mapreduce.Job;

public interface JobProvider {

    Job get() throws Exception;

}
