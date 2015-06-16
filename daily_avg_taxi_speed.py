#Average dialy taxi speed
from pyspark import SparkContext, StorageLevel, SparkConf
import boto
import os
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My nyc taxi app")
         .set("spark.executor.memory", "1g"))

sc = SparkContext(conf=conf)
root_dir = ''

sc.addPyFile(root_dir + 'utils.py')

from utils import parse_taxi_record_avg_speed

# Load Data
raw_data_url = "data/trips-subset.csv"
raw_data = sc.textFile(raw_data_url)

trips = raw_data.map(parse_taxi_record_avg_speed).reduceByKey( lambda a, b: a + b )
trips.persist(StorageLevel.MEMORY_AND_DISK)
#Number of trips
print trips.count()

trips_avg_speed_grouped = trips_avg_speed.map(lambda ((r,c,t),s): (t[6:8],s)).groupByKey()
trips_avg_speed_dialy = trips_avg_speed_grouped.map(lambda x: (x[0], round(sum(x[1])/len(x[1]))))
#average daily taxi speed
print trips_avg_speed_dialy.sortByKey().collect()

