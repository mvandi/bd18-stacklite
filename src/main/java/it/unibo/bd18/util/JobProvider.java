package it.unibo.bd18.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public interface JobProvider {

    final class Builder implements JobProvider {

        private final List<Mapping> mappingList;
        private Class mapOutputKeyClass;
        private Class mapOutputValueClass;

        public Builder() {
            this.mappingList = new LinkedList<>();
        }

        public <K, V> Builder addMapping(Path inputPath, Class<? extends InputFormat> inputFormat, Class<? extends Mapper<?, ?, ? extends  K, ? extends V>> mapper, Class<? extends K> outputKeyClass, Class<? extends V> outputValueClass) {
            mappingList.add(new Mapping(inputPath, inputFormat, mapper));
            mapOutputKeyClass = mapOutputKeyClass == null ? outputKeyClass : ClassHelper.commonSuperclass(mapOutputKeyClass, outputKeyClass);
            mapOutputValueClass = mapOutputValueClass == null ? outputValueClass : ClassHelper.commonSuperclass(mapOutputValueClass, outputValueClass);
            return this;
        }

        @Override
        public Job get() throws IOException {
            return null;
        }

        private static class Mapping {
            final Path inputPath;
            final Class<? extends Mapper> mapper;
            final Class<? extends InputFormat> inputFormat;

            Mapping(Path inputPath, Class<? extends InputFormat> inputFormat, Class<? extends Mapper> mapper) {
                this.inputPath = inputPath;
                this.inputFormat = inputFormat;
                this.mapper = mapper;
            }
        }

    }

    Job get() throws IOException;

}
