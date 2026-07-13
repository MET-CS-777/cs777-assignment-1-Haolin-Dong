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
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Assignment-1")

    rdd = sc.textFile(sys.argv[1])

    rdd_parsed = rdd.map(lambda line: tuple(line.split(',')))
    
    # Apply cleaning filter
    rdd_clean = rdd_parsed.filter(correctRows)
    rdd_clean.cache()

    #Task 1 : Top 10 taxis with highest count of DISTINCT drivers.
    
    # Extract (medallion, hack_license)
    taxi_driver_rdd = rdd_clean.map(lambda x: (x[0], x[1]))
    
    # distinct
    distinct_taxi_drivers = taxi_driver_rdd.distinct()
    
    # Map to count drivers
    medallion_count_rdd = distinct_taxi_drivers.map(lambda x: (x[0], 1))
    
    # Sum up the counts for each medallion
    medallion_driver_counts = medallion_count_rdd.reduceByKey(add)
    
    # Sort by count
    top_10_taxis = medallion_driver_counts.takeOrdered(10, key=lambda x: -x[1])
    
    results_1 = sc.parallelize(top_10_taxis)
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])

    sc.stop()