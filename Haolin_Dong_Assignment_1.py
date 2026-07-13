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
    if len(sys.argv) != 4:
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
    taxi_driver = rdd_clean.map(lambda x: (x[0], x[1]))

    # Get distinct pairs
    distinct_taxi_drivers = taxi_driver.distinct()

    # Map to (medallion, 1) for counting
    medallion_count = distinct_taxi_drivers.map(lambda x: (x[0], 1))

    # Sum up the counts for each medallion
    medallion_driver_counts = medallion_count.reduceByKey(add)

    # Sort by count descending and take top 10
    top_10_taxis = medallion_driver_counts.takeOrdered(10, key=lambda x: -x[1])

    # Save results
    sc.parallelize(top_10_taxis).coalesce(1).saveAsTextFile(sys.argv[2])


    # Task 2: Top 10 drivers by average earned money per minute

    # Extract (driver_id, (total_amount, trip_time))
    driver_stats = rdd_clean.map(lambda x: (x[1], (float(x[16]), float(x[4]))))

    # Aggregate: sum total amount and total time per driver
    driver_aggregated = driver_stats.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Calculate money per minute
    driver_mpm = driver_aggregated.mapValues(lambda x: x[0] / (x[1] / 60.0))

    # Sort descending and take top 10
    top_10_drivers = driver_mpm.takeOrdered(10, key=lambda x: -x[1])

    # Save results
    sc.parallelize(top_10_drivers).coalesce(1).saveAsTextFile(sys.argv[3])


    #Task 3 - Optional
    #Your code goes here

    #Task 4 - Optional
    #Your code goes here


    sc.stop()
    
    # ! spark-submit /content/Assignment_1_code.py gs://met-cs-777-data/taxi-data-sorted-large.csv.bz2 ./output_task1 ./output_task2
    # ! head ./output_task1/part-*
    # ! head ./output_task2/part-*