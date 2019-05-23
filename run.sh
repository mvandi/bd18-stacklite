#!/bin/bash

usage() {
    echo -e "\tusage: $0: <options> <result_path>" >&2
    echo "run options:"
    echo -e "\t--mapreduce\trun job using Apache Hadoop MapReduce" >&2
    echo -e "\t--spark\t\trun job using Apache Spark" >&2
    echo -e "\t--job <number>\tnumber of the job to run" >&2
    echo -e "\t--no-save \t\tdon't save results in local directory" >&2
    exit 1
}

isset() {
    name=$1
    if [ -z ${!name+x} ]; then
        return 1 #variable does not exist
    else
        return 0
    fi
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --mapreduce)
            if isset SPARK; then
                unset SPARK
            fi
            MAPREDUCE=1
            JOBTYPE=mapreduce
            shift #past argument
            ;;
        --spark)
            if isset MAPREDUCE; then
                unset MAPREDUCE
            fi
            SPARK=1
            JOBTYPE=spark
            shift #past argument
            ;;
        --job)
            shift #past argument
            JOB_N=$1
            shift
            ;;
        --no-save)
            NO_SAVE=1
            shift #past argument
            ;;
        *)
            RESULT_PATH=$1
            shift
            ;;
    esac
done

if ! isset RESULT_PATH; then
    usage
fi

if isset JOBTYPE; then
    RESULT_PATH=$RESULT_PATH/$JOBTYPE
else
    usage
fi

if ! isset JOB_N; then
    usage
fi

if isset MAPREDUCE; then
    echo "Running Apache Hadoop MapReduce job..."
    hadoop jar bd18-stacklite.jar it.unibo.bd18.stacklite.mapreduce.job${JOB_N}.Main $RESULT_PATH
elif isset SPARK; then
    echo "Running Apache Spark job..."
    spark2-submit --class it.unibo.bd18.stacklite.spark.Job${JOB_N} bd18-stacklite.jar $RESULT_PATH
else
    usage
fi

if [ "$?" == "0" ]; then
    if ! isset NO_SAVE; then
        RESULT_FILE=results${JOB_N}-${JOBTYPE}.txt
        hdfs dfs -cat $RESULT_PATH/* > $RESULT_FILE
        hdfs dfs -rm -r -skipTrash $RESULT_PATH
        echo "Output written to `pwd`/$RESULT_FILE"
    fi
fi
