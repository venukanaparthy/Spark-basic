from pyspark import SparkContext

baseDir = os.path.join('data')
inputPath = os.path.join('trips.csv')
fileName = os.path.join(baseDir, inputPath)
tripsRDD = sc.textFile(fileName,8).map(parse_trips).reduceByKey(lambda x, y: x+y )

#first 10 trips
trips10RDD = tripsRDD.take(10)
print trips10RDD

#aggregate trips by year
import 
tripsByYearRDD = tripsRDD.map(lambda ((c,r,d),v): (d[:4], v)).reduceByKey(lambda x, y: x+y )
print tripsByYearRDD

#aggregate by month
tripsByMonthRDD = tripsRDD.map(lambda ((c,r,d),v): (d[:4] + '-' + d[4:6], v)).reduceByKey(lambda x, y: x+y)
print tripsByMonthRDD.collect()
