#!/bin/bash

usage() {
    echo "$0: <--spark|--mapreduce> <questions_path> <questiontags_path> <result_path>" >&2
    exit 1
}

if [ "$#" != 4 ]; then
    usage
fi

QUESTIONS_PATH=$2
QUESTIONTAGS_PATH=$3
RESULT_PATH=$4

if [ "$1" == "--mapreduce" ]; then
    MAPREDUCE=1
elif [ "$1" == "--spark" ]; then
    SPARK=1
else
    usage
fi

if [ ! -z ${MAPREDUCE+x} ]; then
    echo "Running Apache Hadoop MapReduce job..."
    hadoop jar bd18-stacklite.jar it.unibo.bd18.stacklite.mapreduce.Job1 $QUESTIONS_PATH $QUESTIONTAGS_PATH $RESULT_PATH
elif [ ! -z ${SPARK+x} ]; then
    echo "Running Apache Spark job..."
    spark2-submit --class it.unibo.bd18.stacklite.spark.Job1 bd18-stacklite.jar $QUESTIONS_PATH $QUESTIONTAGS_PATH $RESULT_PATH
fi
