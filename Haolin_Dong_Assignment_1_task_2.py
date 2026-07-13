from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True

    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles,
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task2 <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")

    rdd = sc.textFile(sys.argv[1])

    rdd_parsed = rdd.map(lambda line: tuple(line.split(',')))
    
    rdd_clean = rdd_parsed.filter(correctRows)
    rdd_clean.cache()


    #Task 2: Top 10 drivers by average earned money per minute.
    
    # Extract (driver_id, (total_amount, trip_time))
    driver_stats_rdd = rdd_clean.map(lambda x: (x[1], (float(x[16]), float(x[4]))))
    
    # Sum up total money and total time for each driver
    driver_aggregated = driver_stats_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    # Money per minute
    driver_mpm_rdd = driver_aggregated.mapValues(lambda x: x[0] / (x[1] / 60.0))
    
    # Sort by money per minute descending and take top 10
    top_10_drivers = driver_mpm_rdd.takeOrdered(10, key=lambda x: -x[1])
    
    results_2 = sc.parallelize(top_10_drivers)
    results_2.coalesce(1).saveAsTextFile(sys.argv[2])


    sc.stop()
