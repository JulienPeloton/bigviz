#!/bin/bash

PACK=com.github.astrolabsoftware:spark-fits_2.11:0.6.0
fn=`pwd`"/test_data.fits"

if [ ! -f $fn ]; then
  python create_point.py -npoints 20000 -filename test_data.fits
fi

# Launch it locally
spark-submit --master local[*] --packages $PACK \
    simple_example.py -fn $fn -hdu 1
