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


def parse_trips(line):
    """Parse a CSV line from Taxi data CSV, used in
    .map call for initial RDD

    Args:
      line (str) - line from CSV to be parsed

    Returns
      ((int, int, str,), int) - represents row, column, period, and count
    """
    (medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag,
    pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs,
    trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude,
    dropoff_latitude) = line.split(',')

    pickup_date, pickup_time = pickup_datetime.split()
    pickup_year, pickup_month, pickup_day = pickup_date.split('-')
    pickup_hour, pickup_minute, _ = pickup_time.split(':')
    xmin, xmax, ymin, ymax = -180.0, 180.0, -90.0, 90.0
    cellsize = 50
    
    try:
        pickup_lat, pickup_lng = float(pickup_latitude), float(pickup_longitude)
    except:
        # There's some bad data here -- default to 0.0 for now'
        pickup_lat, pickup_lng = 0.0, 0.0

    def get_row_col(x, y):
        """Get a cell row and column given a point"""
        col = int((x - xmin) / cellsize)
        row = -1 * int((y - ymax) / cellsize)
        return col, row

    pickup_col, pickup_row = get_row_col(pickup_lat, pickup_lng)

    period = "{pickup_year}{pickup_month:02d}{pickup_day:02d}{pickup_hour:02d}-{trip_distance},{trip_time_in_secs}".format(
        pickup_year=int(pickup_year), pickup_month=int(pickup_month), pickup_day=int(pickup_day),
        pickup_hour=int(pickup_hour), trip_distance=float(trip_distance), trip_time_in_secs=int(trip_time_in_secs))

    return ((pickup_row, pickup_col, period), 1)
