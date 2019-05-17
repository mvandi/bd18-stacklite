#!/bin/bash

usage() {
    echo -e "\tusage: $0: <options> <result_path>" >&2
    echo "run options:"
    echo -e "\t--mapreduce\trun job using Apache Hadoop MapReduce" >&2
    echo -e "\t--spark\t\trun job using Apache Spark" >&2
    echo -e "\t--job\t\tnumber of the job to run" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --mapreduce)
            if [ ! -z ${SPARK+x} ]; then
                unset SPARK 
            fi
            MAPREDUCE=1
            JOBTYPE=mapreduce
            shift # past argument
            ;;
        --spark)
            if [ ! -z ${MAPREDUCE+x} ]; then
                unset MAPREDUCE 
            fi
            SPARK=1
            JOBTYPE=spark
            shift # past argument
            ;;
        --job)
            shift # past argument
            JOB_N=$1
            shift
            ;;
        *)
            RESULT_PATH=$1
            shift
            ;;
    esac
done

if [ -z ${RESULT_PATH+x} ]; then
    usage
fi

if [ ! -z ${JOBTYPE+x} ]; then
    RESULT_PATH=$RESULT_PATH/$JOBTYPE
else
    usage
fi

if [ -z ${JOB_N+x} ]; then
    usage
fi

if [ ! -z ${MAPREDUCE+x} ]; then
    echo "Running Apache Hadoop MapReduce job..."
    hadoop jar bd18-stacklite.jar it.unibo.bd18.stacklite.mapreduce.job${JOB_N}.Main $RESULT_PATH
elif [ ! -z ${SPARK+x} ]; then
    echo "Running Apache Spark job..."
    spark2-submit --class it.unibo.bd18.stacklite.spark.Job${JOB_N} bd18-stacklite.jar $RESULT_PATH
else
    usage
fi

if [ "$?" == "0" ]; then
    RESULT_FILE=results-${JOBTYPE}.txt
    hdfs dfs -cat $RESULT_PATH/* > $RESULT_FILE
    hdfs dfs -rm -r -skipTrash $RESULT_PATH
    echo "Output written to `pwd`/$RESULT_FILE"
fi
