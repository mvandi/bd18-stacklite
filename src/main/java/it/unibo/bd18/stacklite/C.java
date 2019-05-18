package it.unibo.bd18.stacklite;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;

public final class C {

    public static final class dates {
        public static final Date startDate = Utils.readDate("2012-01-01T00:00:00Z");
        public static final Date endDate = Utils.readDate("2014-12-31T23:59:59Z");

        private dates() {
        }
    }

    public static final class tuning {
        public static final class cpu {
            public static final int nodeCount = 10;

            public static final int availableCores = 4;

            public static final int executorCores = 3;

            public static final int executorsPerNode = (availableCores - 1) / executorCores;

            public static final int executorCount = nodeCount * executorsPerNode - 1;

            private cpu() {
            }
        }

        public static final class memory {
            public static final float offHeapSize = 0.1f;

            public static final int availableMemory = 16;

            public static final int executorMemory = Math.round((availableMemory * 0.75f) * ((1 - offHeapSize) / cpu.executorsPerNode));

            private memory() {
            }
        }

        private tuning() {
        }
    }

    public static final class hdfs {
        public static final String basePath = "/user/mvandi/stacklite";

        public static final class data {
            private static final String basePath = hdfs.basePath + "/data";

            public static final String questions = basePath + "/questions.csv";

            public static final String questionTags = basePath + "/question_tags.csv";

            private data() {
            }
        }

        private hdfs() {
        }
    }

    private C() {
    }

    public static final class parquet {
        public static final String basePath = hdfs.basePath + "/parquet-tables";

        public static final class tables {
            public static final String questions = "questions";

            public static final String questionTags = "question_tags";

            private tables() {
            }
        }

        public static String table(String tableName) {
            return parquet.basePath + "/" + tableName;
        }

        public static boolean tableExists(FileSystem fs, String tableName) throws IOException {
            return fs.exists(new Path(basePath + "/" + tableName));
        }

        public static boolean create(FileSystem fs) throws IOException {
            final Path parquetPath = new Path(basePath);
            if (!fs.exists(parquetPath)) {
                fs.create(parquetPath, false);
            }
            return false;
        }

        private parquet() {
        }
    }

}
