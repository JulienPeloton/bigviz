#!/bin/bash

PACK=com.github.astrolabsoftware:spark-fits_2.11:0.6.0
fn=`pwd`"/test_data.fits"

# If the file doesn't exist locally, create it and copy it to HDFS
if [ ! -f $fn ]; then
  python create_point.py -npoints 2000000 -filename test_data.fits
  hdfs dfs -put test_data.fits
fi

# URI for HDFS
NAME=`whoami`
fn="hdfs:///user/${NAME}/test_data.fits"

# Launch it on 2 executors (34 cores)
spark-submit --master spark://134.158.75.222:7077 --packages $PACK \
  --driver-memory 4g --executor-memory 30g \
  --executor-cores 17 --total-executor-cores 34 \
  simple_example.py -fn $fn -hdu 1
