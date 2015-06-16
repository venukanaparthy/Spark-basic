#rdd utils

def parse_taxi_record_avg_speed(line):
    """Parse a CSV line from Taxi data CSV, used in
    .map call for initial RDD

    Args:
      line (str) - line from CSV to be parsed

    Returns
      ((int, int, str,), float) - represents row, column, period, and speed
    """
    
    (medallion, hack_license, vendor_id, rate_code, store_and_fwd_flag,
    pickup_datetime, dropoff_datetime, passenger_count, trip_time_in_secs,
    trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude,
    dropoff_latitude) = line.split(',')
   
    pickup_date, pickup_time = pickup_datetime.strip().split()
    pickup_year, pickup_month, pickup_day = pickup_date.split('-')
    pickup_hour, pickup_minute, _ = pickup_time.split(':')
    
    trip_time_in_secs_val, trip_distance_val = int(trip_time_in_secs), float(trip_distance)
    if trip_distance_val == 0 or trip_time_in_secs_val == 0:
        speed =0
    else:
        speed = (trip_distance_val * 60 * 60)/trip_time_in_secs_val
    
    try:
        pickup_lat, pickup_lng = coords_to_utm(pickup_latitude, pickup_longitude)
    except:
        # There's some bad data here -- default to 0.0 for now'
        pickup_lat, pickup_lng = 0.0, 0.0

    def get_row_col(x, y):
        """Get a cell row and column given a point"""
        col = int((x - raster_extent.x_min) / raster_extent.cell_width)
        row = -1 * int((y - raster_extent.y_max) / raster_extent.cell_height)
        return col, row

    pickup_col, pickup_row = get_row_col(pickup_lat, pickup_lng)
	
    
    period = "{pickup_year}{pickup_month:02d}{pickup_day:02d}{pickup_hour:02d}".format(
        pickup_year=int(pickup_year), pickup_month=int(pickup_month), pickup_day=int(pickup_day),
        pickup_hour=int(pickup_hour))
		
	
    return ((pickup_row, pickup_col, period), speed)
