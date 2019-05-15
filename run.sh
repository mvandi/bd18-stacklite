#!/bin/bash

usage() {
    echo -e "\tusage: $0: <options> <questions_path> <questiontags_path> <result_path>" >&2
    echo "run options:"
    echo -e "\t--mapreduce\trun job using Apache Hadoop MapReduce" >&2
    echo -e "\t--spark\t\trun job using Apache Spark" >&2
    exit 1
}

if [ "$#" != 4 ]; then
    usage
fi

QUESTIONS_PATH=$2
QUESTIONTAGS_PATH=$3

if [ "$1" == "--mapreduce" ]; then
    MAPREDUCE=1
    JOBTYPE=mapreduce
elif [ "$1" == "--spark" ]; then
    SPARK=1
    JOBTYPE=spark
else
    usage
fi

RESULT_PATH=$4/$JOBTYPE

if [ ! -z ${MAPREDUCE+x} ]; then
    echo "Running Apache Hadoop MapReduce job..."
    hadoop jar bd18-stacklite.jar it.unibo.bd18.stacklite.mapreduce.job1.Main $QUESTIONS_PATH $QUESTIONTAGS_PATH $RESULT_PATH
elif [ ! -z ${SPARK+x} ]; then
    echo "Running Apache Spark job..."
    spark2-submit --class it.unibo.bd18.stacklite.spark.Job1 bd18-stacklite.jar $QUESTIONS_PATH $QUESTIONTAGS_PATH $RESULT_PATH
fi

if [ "$?" == "0" ]; then
    RESULT_FILE=results-${JOBTYPE}.txt
    hdfs dfs -cat $RESULT_PATH/* > $RESULT_FILE
    hdfs dfs -rm -r -skipTrash $RESULT_PATH
    echo "Output written to `pwd`/$RESULT_FILE"
fi
