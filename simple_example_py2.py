# Copyright 2018 Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pyspark.sql import SparkSession

import argparse

def quiet_logs(sc, log_level="ERROR"):
    """
    Set the level of log in Spark.

    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.

    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)

def addargs(parser):
    """ Parse command line arguments for simple_example.py """

    ## Arguments
    parser.add_argument(
        '-fn', dest='fn',
        required=True,
        help='Path to a FITS file')

    ## Arguments
    parser.add_argument(
        '-hdu', dest='hdu',
        required=True,
        help='HDU index to load.')


if __name__ == "__main__":
    """
    Visualise the elements of a spatial RDD
    """
    parser = argparse.ArgumentParser(
        description="""
        Visualise the elements of a spatial RDD
        """)
    addargs(parser)
    args = parser.parse_args(None)

    # Initialize the Spark Session
    spark = SparkSession\
        .builder\
        .getOrCreate()

    # Set logs to be quiet
    quiet_logs(spark.sparkContext, log_level="OFF")

    # Load the data inside a DataFrame
    df = spark.read.format("fits").option("hdu", args.hdu).load(args.fn)

    # Apply a collapse function
    # Before, repartition our DataFrame to mimick a large data set.
    data = df.repartition(256).rdd.collect()

    # Re-organise the data into lists of x, y, z coordinates
    x = [p[0] for p in data if p is not None]
    y = [p[1] for p in data if p is not None]
    z = [p[2] for p in data if p is not None]

    # This is the place where you will pass those lists to the C routines.
    # Alternatively, you could save the data on disk and load it inside the
    # C program.
    import hello_ext
    print(hello_ext.greet())
