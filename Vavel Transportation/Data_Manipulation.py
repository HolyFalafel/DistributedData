import MySQLdb
import time
from math import radians, cos, sin, asin, sqrt


# connection
def conn_MySQL():

    db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                         user="root",         # your username
                         passwd="Ar050783",  # your password
                         db="warzawdata")        # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need

    return db

def calc_distance_using_haversine(lon1, lat1, lon2, lat2):
    """
    :param lon1: longitude point 1
    :param lat1: latitude point 1
    :param lon2: longitude point 2
    :param lat2: latitude  point 2
    :return: Calculate the great circle distance between two points on the earth (specified in decimal degrees)
    http://stackoverflow.com/questions/15736995/how-can-i-quickly-estimate-the-distance-between-two-latitude-longitude-points
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def calc_distance_using_equirectangular(lon1, lat1, lon2, lat2):
    """
    :param lon1: longitude point 1
    :param lat1: latitude point 1
    :param lon2: longitude point 2
    :param lat2: latitude  point 2
    :return: equi-rectangular distance between 2 points
    http://stackoverflow.com/questions/15736995/how-can-i-quickly-estimate-the-distance-between-two-latitude-longitude-points
    """
    r = 6371 # radius of the earth in km
    x = (lon2 - lon1) * cos(0.5 * (lat2 + lat1))
    y = lat2 - lat1
    km = r * sqrt(x * x + y * y)
    return km

def main():                      # Define the main function

    db = conn_MySQL()
    cur = db.cursor()

    # Calculate Y:
    # Get time to next stop
    # And maybe time to last stop
    # ==============================================
    # get trips ordered by course (trip) ID and Time
    sql = "SELECT b.Time, UNIX_TIMESTAMP(b.Time) as time_stamp, b.courseIdentifier, " \
          "b.NearestStop, b.previousStop, " \
          "b.previousStopArrivalTime, b.previousStopLeaveTime, b.nextStop, " \
          "b.FileName, b.RowNum " \
          "FROM `vavel-warsaw`.brigades_data b " \
          "where b.TramStatus = 'STOPPED' " \
          "and b.timetableStatus <> 'MISSING' " \
          "order by b.courseIdentifier, b.Time "
    cur.execute(sql)

    raw_data = cur.fetchall()
    # prev record data
    prev_rec_course_id = -1
    prev_rec_time_stamp = 0
    prev_rec_nearest_stop = 0
    prev_rec_previous_stop = 0
    prev_rec_next_stop = 0
    # time took from trip start
    time_of_trip = 0
    num_of_stops_counted = 0
    prev_file_name = 0
    prev_row_rum = 0

    for time_str, time_stamp, course_id, nearest_stop, previous_stop, previousStopArrivalTime, previousStopLeaveTime, next_stop, file_name, row_rum in raw_data:
        print time_str, time_stamp, course_id, nearest_stop, previous_stop, previousStopArrivalTime, previousStopLeaveTime, next_stop, file_name, row_rum

        # reached a new trip
        if course_id != prev_rec_course_id:
            time_of_trip = 0
            prev_rec_course_id = course_id
            prev_rec_time_stamp = 0
            num_of_stops_counted = 0
        # still on the same trip
        else:
            time_delta = time_stamp - prev_rec_time_stamp
            time_of_trip += time_delta
            # found a wanted transfer between two stops
            # i.e reached to another stop - we want to save time delta and time of trip
            if prev_rec_next_stop == nearest_stop or prev_rec_next_stop == previous_stop:
                print "time_delta: ", time_delta
                print "time_of_trip: ", time_of_trip
                num_of_stops_counted += 1
                print "num_of_stops_counted: ", num_of_stops_counted
                # todo: consider adding "aerial" distance between stops

        # saving previous record data
        prev_rec_time_stamp = time_stamp
        prev_rec_nearest_stop = nearest_stop
        prev_rec_previous_stop = previous_stop
        prev_rec_next_stop = next_stop
        prev_file_name = file_name
        prev_row_rum = row_rum
        # Working with timestamps in MySQL
    unix_timestamp_query = 'select UNIX_TIMESTAMP(Time) ' \
    'from `vavel-warsaw`.brigades_data'


main()                           # Invoke the main function
