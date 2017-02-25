import MySQLdb
import time
from math import radians, cos, sin, asin, sqrt

# we'll use this dictionary to update the data in the db
    # key is (file name, row number)
    record_update_data = {}

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

def save_data_in_db(cursor, file_name, row_num, time_delta, time_to_last_stop):
    # filename's supposed to be string
    if isinstance(file_name, int):
        return
    # print 'update data: ', file_name, row_num, time_delta, time_to_last_stop
    update_query = 'UPDATE `vavel-warsaw`.`brigades_data` ' \
                   'SET ' \
                   '`timeToNextStop` = %s' \
                   ', `timeToLastStop` = %s ' \
                   'WHERE `FileName` = "%s" AND `RowNum` = %s;' % (str(time_delta), str(time_to_last_stop), file_name, str(row_num))

    # print 'query: ', update_query
    cursor.execute(update_query)

def update_trip_in_db(cur, trip_list, time_of_trip):
    # num of trip records saved in db
    num_of_trip_recs_saved = 0

    # update record_update_data dictionary and then call the update db function
    for rec_file_name, rec_row_num, time_to_next_stop, time_to_this_stop in trip_list:
        # store time to stop (trip time) in trip data tuple
        time_to_last_stop = time_of_trip - time_to_this_stop
        record_update_data[(rec_file_name, rec_row_num)] = (time_to_next_stop, time_to_last_stop)

        # now update the db.........
        save_data_in_db(cur, rec_file_name, rec_row_num, time_to_next_stop, time_to_last_stop)

        num_of_trip_recs_saved += 1

    return num_of_trip_recs_saved

def main():                      # Define the main function

    db = conn_MySQL()
    cur = db.cursor()

    # Calculate Y:
    # Get time to next stop
    # And maybe time to last stop
    # ==============================================
    # get trips ordered by course (trip) ID and Time
    sql = "SELECT b.Time, UNIX_TIMESTAMP(b.Time) as time_stamp, b.courseIdentifier, " \
          "b.timetableIdentifier, b.tripID, b.NearestStop, b.previousStop, " \
          "b.previousStopArrivalTime, b.previousStopLeaveTime, b.nextStop, " \
          "b.FileName, b.RowNum " \
          "FROM `vavel-warsaw`.brigades_data b " \
          "where b.TramStatus = 'STOPPED' " \
          "and b.timetableStatus <> 'MISSING' " \
          "order by b.tripID, b.Time "
    cur.execute(sql)

    raw_data = cur.fetchall()

    # prev record data
    prev_rec_course_id = -1
    prev_rec_timetable_id = -1
    prev_rec_trip_id = -1
    prev_rec_time_stamp = 0
    prev_rec_nearest_stop = 0
    prev_rec_previous_stop = 0
    prev_rec_next_stop = 0
    prev_rec_file_name = 0
    prev_rec_row_num = 0
    # time took from previous stop
    time_delta = 0
    # time took from trip start
    time_of_trip = 0
    stop_num = 0
    trip_num = 0

    # num of trip records saved in db
    num_of_trip_recs_saved = 0



    # stops reported records in trip
    # key is trip id
    trip_data = {}

    for time_str, time_stamp, course_id, timetable_id, trip_id, nearest_stop, previous_stop, previousStopArrivalTime, previousStopLeaveTime, next_stop, file_name, row_num in raw_data:

        # reached a new trip
        # if course_id != prev_rec_course_id:
        # if timetable_id != prev_rec_timetable_id:
        if trip_id != prev_rec_trip_id:

            # adding a list for trip data
            trip_data[trip_id] = []

            # avoiding the first trip - which has no data
            if trip_num != 0:
                # update record_update_data dictionary and then update in db
                num_of_trip_recs_saved += update_trip_in_db(cur, trip_data[prev_rec_trip_id], time_of_trip)

            time_delta = time_of_trip = 0
            prev_rec_course_id = course_id
            prev_rec_timetable_id = timetable_id
            prev_rec_trip_id = trip_id
            prev_rec_time_stamp = 0
            stop_num = 0
            trip_num += 1
        # still on the same trip
        else:
            time_delta = time_stamp - prev_rec_time_stamp
            time_of_trip += time_delta
            # found a wanted transfer between two stops
            # i.e reached to another stop - we want to save time delta and time of trip
            if prev_rec_next_stop == nearest_stop or prev_rec_next_stop == previous_stop:
                # print "time_delta: ", time_delta, ", in minutes: ", time_delta / 60, ":", time_delta % 60
                # print "time_of_trip: ", time_of_trip, ", in minutes: ", time_of_trip / 60, ":", time_of_trip % 60
                stop_num += 1
                print "trip_num: ", trip_num, ", num_of_stops_counted: ", stop_num
                # todo: consider adding "aerial" distance between stops

            # adding record data - time data for previous stop: rec_id, time to next stop, time to current stop
            trip_data[trip_id].append((prev_rec_file_name, prev_rec_row_num, time_delta, time_of_trip - time_delta))

        print time_delta / 60, ":", time_delta % 60, time_of_trip / 60, ":", time_of_trip % 60, time_str, time_stamp, course_id, timetable_id, trip_id, nearest_stop, previous_stop, previousStopArrivalTime, previousStopLeaveTime, next_stop, file_name, row_num
        # saving previous record data
        prev_rec_time_stamp = time_stamp
        prev_rec_nearest_stop = nearest_stop
        prev_rec_previous_stop = previous_stop
        prev_rec_next_stop = next_stop
        prev_rec_file_name = file_name
        prev_rec_row_num = row_num

    print "num of db saves: ", num_of_trip_recs_saved

    db.commit()
    db.close()

main()                           # Invoke the main function
