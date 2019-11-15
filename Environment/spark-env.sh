#!/usr/bin/env bash
##
# Generated by Cloudera Manager and should not be modified directly
##

SELF="$(cd $(dirname $BASH_SOURCE) && pwd)"
if [ -z "$SPARK_CONF_DIR" ]; then
  export SPARK_CONF_DIR="$SELF"
fi

export SPARK_HOME=/opt/cloudera/parcels/SPARK2-2.3.0.cloudera4-1.cdh5.13.3.p0.611179/lib/spark2

SPARK_PYTHON_PATH=""
if [ -n "$SPARK_PYTHON_PATH" ]; then
  export PYTHONPATH="$PYTHONPATH:$SPARK_PYTHON_PATH"
fi

export HADOOP_HOME=/opt/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/hadoop
export HADOOP_COMMON_HOME="$HADOOP_HOME"

if [ -n "$HADOOP_HOME" ]; then
  LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${HADOOP_HOME}/lib/native
fi

SPARK_EXTRA_LIB_PATH=""
if [ -n "$SPARK_EXTRA_LIB_PATH" ]; then
  LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SPARK_EXTRA_LIB_PATH
fi

export LD_LIBRARY_PATH

HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$SPARK_CONF_DIR/yarn-conf}
export HADOOP_CONF_DIR

PYLIB="$SPARK_HOME/python/lib"
if [ -f "$PYLIB/pyspark.zip" ]; then
  PYSPARK_ARCHIVES_PATH=
  for lib in "$PYLIB"/*.zip; do
    if [ -n "$PYSPARK_ARCHIVES_PATH" ]; then
      PYSPARK_ARCHIVES_PATH="$PYSPARK_ARCHIVES_PATH,local:$lib"
    else
      PYSPARK_ARCHIVES_PATH="local:$lib"
    fi
  done
  export PYSPARK_ARCHIVES_PATH
fi

# Spark uses `set -a` to export all variables created or modified in this
# script as env vars. We use a temporary variables to avoid env var name
# collisions.
# If PYSPARK_PYTHON is unset, set to CDH_PYTHON
TMP_PYSPARK_PYTHON=${PYSPARK_PYTHON:-'/opt/cloudera/parcels/Anaconda-5.1.0.1/bin/python'}
# If PYSPARK_DRIVER_PYTHON is unset, set to CDH_PYTHON
TMP_PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-/opt/cloudera/parcels/Anaconda-5.1.0.1/bin/python}

if [ -n "$TMP_PYSPARK_PYTHON" ] && [ -n "$TMP_PYSPARK_DRIVER_PYTHON" ]; then
  export PYSPARK_PYTHON="$TMP_PYSPARK_PYTHON"
  export PYSPARK_DRIVER_PYTHON="$TMP_PYSPARK_DRIVER_PYTHON"
fi

# Add the Kafka jars configured by the user to the classpath.
SPARK_DIST_CLASSPATH=
SPARK_KAFKA_VERSION=${SPARK_KAFKA_VERSION:-'0.9'}
case "$SPARK_KAFKA_VERSION" in
  0.9)
    SPARK_DIST_CLASSPATH="$SPARK_HOME/kafka-0.9/*"
    ;;
  0.10)
    SPARK_DIST_CLASSPATH="$SPARK_HOME/kafka-0.10/*"
    ;;
  None)
    ;;
  *)
    echo "Invalid Kafka version: $SPARK_KAFKA_VERSION"
    exit 1
    ;;
esac

export SPARK_DIST_CLASSPATH="$SPARK_DIST_CLASSPATH:$(paste -sd: "$SELF/classpath.txt")"
 if [ "${RGZ_ENV}" == "True" ]; then 
  export PYSPARK_DRIVER_PYTHON=/home/hduser/.virtualenvs/ClaRAN/bin/python
  export PYSPARK_PYTHON=/home/hduser/.virtualenvs/ClaRAN/bin/python

fi

 if [ "${RGZ3_ENV}" == "True" ]; then 
  export PYSPARK_DRIVER_PYTHON=/home/hduser/.virtualenvs/ClaRAN3/bin/python
  export PYSPARK_PYTHON=/home/hduser/.virtualenvs/ClaRAN3/bin/python

fi

 if [ "${SparkCookbook}" == "True" ]; then 
  export PYSPARK_DRIVER_PYTHON=/home/hduser/.virtualenvs/SparkCookbook/bin/python
  export PYSPARK_PYTHON=/home/hduser/.virtualenvs/SparkCookbook/bin/python

fi

 if [ "${SPARK_FITS}" == "True" ]; then 
  export PYSPARK_DRIVER_PYTHON=/home/hduser/.virtualenvs/SparkFits/bin/python
  export PYSPARK_PYTHON=/home/hduser/.virtualenvs/SparkFits/bin/python
fi

if [ "${ELEPHAS}" == "True" ]; then 
  export PYSPARK_DRIVER_PYTHON=/home/hduser/.virtualenvs/Elephas/bin/python
  export PYSPARK_PYTHON=/home/hduser/.virtualenvs/Elephas/bin/python
fi


