#!/bin/bash

PACK=com.github.astrolabsoftware:spark-fits_2.11:0.6.0
fn=`pwd`"/test_data.fits"

# Build the plugin
python setup.py build_ext --inplace

if [ ! -f $fn ]; then
  python create_point.py -npoints 20000 -filename test_data.fits
fi

# Uncomment for python2
# export PYSPARK_PYTHON=python2.7

# Launch it locally
spark-submit --master local[*] --packages $PACK \
    --files hello_ext.cpython-36m-darwin.so \
    simple_example_py2.py -fn $fn -hdu 1
