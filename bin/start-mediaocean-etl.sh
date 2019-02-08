#!/bin/bash

display_usage() {
    echo "Usage: $0 [-a|--app-name py file with path relative to app/ dir] [-p|--pyspark-python override pipenv --py]"
    echo "[-c|--total-executor-cores] [-d|--driver-memory] [-m|--executor-memory]"
}

CWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

### default assignments
EXECUTOR_CORES=10
DRIVER_MEMORY=8g
JARS='/home/spark/jars/postgresql-42.2.2.jar'
EXECUTOR_MEMORY=8g
NUM_EXECUTORS=1
PYSPARK_DRIVER_PYTHON=$CWD/../.venv/bin/python # NOTE assumes script is in $PROJECT/bin
PYSPARK_PYTHON=/bin/python3.6

### argument vars
# any non-specified arguments must be passed AFTER these or will be ignored
if [ $# -eq 0 ]
then
    echo "ERROR: too few arguments"
    display_usage
    exit 1
fi
while [ "$1" != "" ]; do
    case $1 in
        # app path relative to ./ == .../vampy-adh-etl/app
        -a | --app )                    shift
                                        APP_PATH=$1
                                        ;;
        # spark options
        -c | --total-executor-cores )   shift
                                        EXECUTOR_CORES=$1
                                        ;;
        -d | --driver-memory )          shift
                                        DRIVER_MEMORY=$1
                                        ;;
        -j | --jars )                   shift
                                        JARS=$1
                                        ;;
        -m | --executor-memory )        shift
                                        EXECUTOR_MEMORY=$1
                                        ;;
        -n | --num-executors )          shift
                                        NUM_EXECUTORS=$1
                                        ;;
        # python interpreter
        -p | --pyspark-python )         shift
                                        PYSPARK_PYTHON=$1
                                        ;;
        -x | --extra-args )             shift
                                        EXTRA_ARGS=${@}
                                        ;;
    esac
    shift
done

## any arguments not parsed above are literally appended to the spark-submit (after app name)
#EXTRA_ARGS=${@}

# TODO using --py-files breaks stuff now.
# both boto3 and psycopg2 have issues.
# see discussion in #team-ops. this is being temporarily removed for now
#echo "Zipping site-packages"
#cd ./.venv/lib/python3.6/site-packages
#`find . | grep -v psycopg2 |  zip -@ -r --filesync $CWD/site.zip > /dev/null`
#zip -r --filesync $CWD/site.zip ./* > /dev/null


### execution
cd $CWD
echo "--- Starting Vampy ADH ETL: $APP_PATH ---"
echo "PYSPARK PYTHON: $PYSPARK_PYTHON"
export PYSPARK_PYTHON=$PYSPARK_PYTHON

echo "PYSPARK DRIVER PYTHON: $PYSPARK_DRIVER_PYTHON"
export PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON
set -x # show formatted command

 #--py-files site.zip\ # see above info on zipping
spark-submit\
 --jars "$JARS"\
 --master spark://spark-compute-master-001.prod.dc3:7077\
 --total-executor-cores $EXECUTOR_CORES\
 --driver-memory $DRIVER_MEMORY\
 --executor-memory $EXECUTOR_MEMORY\
 --conf spark.max.cores=$(($EXECUTOR_CORES * $NUM_EXECUTORS))\
 $APP_PATH $EXTRA_ARGS
