__author__ = 'Arik Senderovich'


# import mysql.connector
import MySQLdb
import csv, pyodbc
from datetime import *
from os.path import exists
import numpy
import datetime
from pytz import *
from pulp import *
from collections import namedtuple
from decimal import Decimal
import numpy
from scipy.stats import norm
import math
from collections import OrderedDict
from operator import itemgetter
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import networkx as nx
from haversine import haversine
import operator
import datetime


def conn_MySQL():

    db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                         user="root",         # your username
                         passwd="Ar050783",  # your password
                         db="warzawdata")        # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need

    return db


def remove_space_outliers(GPS_log):
    #Time fixed (or almost fixed), space changes too much. e.g. bus at the same time in different places
    outliers = []
    cur_vehicle = GPS_log[0].vehicle_ID
    v_max =1666 #max velocity of 100 km/h
    max_drift = 20
    for k,t in enumerate(GPS_log):
            print "Outlier detection: "+str(k)+" out of " +str(len(GPS_log))
            if cur_vehicle==t.vehicle_ID:
                cur_vehcile = t.vehicle_ID
                #v_max is 100 km/h
                if t.prev_duration>0 and t.prev_distance/t.prev_duration > v_max:
                    outliers.append(t)
                #Traveling without moving
                elif t.prev_duration ==0 and t.prev_distance > max_drift:
                    outliers.append(t)
                #Duplicate recording
                elif t.prev_duration ==0 and t.prev_distance==0:
                    outliers.append(t)


    #Decide what to do with outliers.

    for o in outliers:
        GPS_log.remove(o)
    return GPS_log


def atStop(t, bus_stops):
    potential_station = namedtuple('BUS_Record', 'LAT, LON, Name, Code')
    potential_station.Code = -1
    potential_distance = 300000
    station_thresh = 100
    #Threshold for velocity
    velocity_thresh = 0.55 #1.38 # meters / seconds why? 5 km/h = 5000 m / h = 83 meters / minute
    avg_velocity = 0
    if t.prev_duration > 0:
        avg_velocity = t.prev_distance/t.prev_duration
    #TODO: problem: might locate stations that are in the opposite directions.
    if avg_velocity<velocity_thresh:
            for s in bus_stops:
                point1 = (s.LAT, s.LON)
                point2 = (t.LAT, t.LON)
                #Distance to station in meters.
                station_distance =haversine(point1,point2)*1000
                if station_distance <=station_thresh:
                    if station_distance < potential_distance:
                        potential_station = s
                        potential_distance = station_distance
    return potential_station


def find_sources(stations,durations):
    #function finds the two longest stations
    if len(stations)!=len(durations) or len(stations)==0 or len(durations)==0:
        return ValueError
    pair = []
    pairs =[]
    for i,d in enumerate(durations):
        pair.append(stations[i])
        pair.append(durations[i])
        pairs.append(pair)
        pair =[]

    if len(pairs)==0 or len(pairs)==1:
        print 'Err'
        return ValueError


    pairs = sorted(pairs, key=itemgetter(1), reverse= True)
    sd = [pairs[0][0],pairs[1][0]]
    sd= sorted(sd, key=itemgetter(0))
    return sd
def find_sources_(stations,durations, counter):
    #function finds the two longest stations
    if len(stations)!=len(durations) or len(stations)==0 or len(durations)==0:
        return ValueError
    pair = []
    pairs =[]
    for i,d in enumerate(durations):
        pair.append(stations[i])
        pair.append(durations[i])
        pairs.append(pair)
        pair =[]

    if len(pairs)==0 or len(pairs)==1:
        print 'Err'
        return ValueError


    pairs = sorted(pairs, key=itemgetter(1), reverse= True)
    sd = [pairs[0][0],pairs[1][0]]
    sd= sorted(sd, key=itemgetter(0))
    return sd

def assign_direction(GPS_log, journey, s_d, trip_id):
    direction = 1
    prev_station = -1
    #trip_id = 0
    cur_state = 2
    hitting_flag = False
    cur_id = GPS_log[0].vehicle_ID

    for k,g in enumerate(GPS_log):

        try:
            j_ind = journey.index(g.vehicle_ID)
            if len(s_d)<2:
                print 'Here'
            else:
                source = s_d[j_ind][0]
                dest = s_d[j_ind][1]

            #Hitting a terminal station, including first row being terminal. Hitting terminal changes direction

            if ((g.station == source or g.station==dest) and g.station!=prev_station) or (g.vehicle_ID!=cur_id):
                g.state = 2
                cur_state = g.state
                trip_id+=1

                if direction==1:
                        direction=2
                else:
                        direction=1
            #  at Terminal - does not change direction
            elif cur_state==2 and g.station==prev_station:
                 g.state = 2
            else:
                cur_state = g.state

            g.direction = direction
            g.trip_ID = trip_id
            cur_id = g.vehicle_ID

            prev_station = g.station

        except ValueError:
            print 'Skipping line ' + str(g.vehicle_ID)
            g.direction = 0
            g.trip_ID = 0

        #I hit a source or destination


        #Hitting terminal station.



    return trip_id

def print_log(GPS_log):

    cur_trip = GPS_log[0].trip_ID
    for g in GPS_log:
        if cur_trip==g.trip_ID:
            print("Trip = "+str(g.trip_ID), " Station="+str(g.station), " Direction="+str(g.direction)," State="+str(g.state))
            cur_trip = g.trip_ID
        else:

            raw_input("Press Enter to continue...")
            cur_trip = g.trip_ID
    return



#todo: assign directions/terminal stations without looking at directions, but incorporate counts (rare depots)
def create_journey_log(GPS_log,bus_stops, trip_id):

    #GPS: id, time, LAT, LON, first_line,  brigade
    #stops: LAT, LON, Name, Code
    #GPS_log = sorted(GPS_log, key=lambda GPS_record: (int(GPS_record.first_line), int(GPS_record.brigade), GPS_record.time1))
    #GPS_log = remove_space_outliers(GPS_log)
    stations_list = []
    total_durations = []
    total_counter = []
    cur_id = -1
    s_d = []
    terminals = []
    journeyed = []
    prev_station=-1

    for k,t in enumerate(GPS_log):
    #100 meters proximity is bus stop (atStop=1), Then move to states (e.g. congestion, terminal station,...)

        print "Log creation: "+str(k)+" out of " +str(len(GPS_log))
        if cur_id !=t.vehicle_ID:
           if len(stations_list)==0 and k>0:
               #print "Line "+str(cur_id) + " never stopped"
               print("Line "+str(cur_id) + " never stopped")
           elif k>0:
            try:
                #Checking if there are distinct source dest.
                s_d = find_sources(stations_list, total_counter)
                if s_d == ValueError:
                    pass
                else:
                    journeyed.append(cur_id)
                    terminals.append(s_d)
            except ValueError:
                pass
           cur_id = t.vehicle_ID
           prev_station=-1
           total_durations = []
           total_counter = []
           stations_list = []

        s = atStop(t,bus_stops)

        #is at stop

        if s.Code !=-1:

            t.atStop = 1
            t.station = s.Code
            t.state = 1
            t.station_LAT = s.LAT
            t.station_LON = s.LON
            #print t.vehicle_ID


            try:

                ind = stations_list.index(s.Code)
                #Make sure that prev duration is after some outlier removing procedure.
                if t.station==prev_station:
                    total_durations[ind]+=t.prev_duration
                    total_counter[ind]+=1

            except ValueError:
                    stations_list.append(t.station)
                    total_durations.append(0)
                    total_counter.append(0)
                    #At stop = state 1
            prev_station = s.Code
        else:

            t.atStop = 0
            t.station = 0
            t.state = 0
            t.station_LAT = -1
            t.station_LON = -1
            prev_station = 0

    if len(stations_list)==0:
               #print "Line "+str(cur_id) + " never stopped"
        print("Line "+str(cur_id) + " never stopped")
    else:
        try:
                #Checking if there are distinct source dest.
            s_d = find_sources(stations_list,total_counter)
            journeyed.append(cur_id)
            terminals.append(s_d)
        except ValueError:
                pass

    #trip_id = assign_direction(GPS_log,journeyed,terminals, trip_id)
    #print_log(GPS_log)

    trip_id = assign_direction(GPS_log, journeyed, terminals, trip_id)
    print 'yeeay'
    return [GPS_log, trip_id]



def create_journey_log_current(GPS_log,bus_stops, trip_id):

    #GPS: id, time, LAT, LON, first_line,  brigade
    #stops: LAT, LON, Name, Code
    #GPS_log = sorted(GPS_log, key=lambda GPS_record: (int(GPS_record.first_line), int(GPS_record.brigade), GPS_record.time1))
    #GPS_log = remove_space_outliers(GPS_log)
    stations_list = []
    total_durations = []
    total_counter = []
    cur_id = -1
    s_d = []
    terminals = []
    journeyed = []
    prev_station=-1

    for k,t in enumerate(GPS_log):
    #100 meters proximity is bus stop (atStop=1), Then move to states (e.g. congestion, terminal station,...)

        print "Log creation: "+str(k)+" out of " +str(len(GPS_log))
        if cur_id !=t.vehicle_ID:
           if len(stations_list)==0 and k>0:
               #print "Line "+str(cur_id) + " never stopped"
               print("Line "+str(cur_id) + " never stopped")
           elif k>0:
            try:
                #Checking if there are distinct source dest.
                s_d = find_sources(stations_list, total_counter)
                if s_d == ValueError:
                    pass
                else:
                    journeyed.append(cur_id)
                    terminals.append(s_d)
            except ValueError:
                pass
           cur_id = t.vehicle_ID
           prev_station=-1
           total_durations = []
           total_counter = []
           stations_list = []

        s = atStop(t,bus_stops)

        #is at stop

        if s.Code !=-1:

            t.atStop = 1
            t.station = s.Code
            t.state = 1
            t.station_LAT = s.LAT
            t.station_LON = s.LON
            #print t.vehicle_ID


            try:

                ind = stations_list.index(s.Code)
                #Make sure that prev duration is after some outlier removing procedure.
                if t.station==prev_station:
                    total_durations[ind]+=t.prev_duration
                    total_counter[ind]+=1

            except ValueError:
                    stations_list.append(t.station)
                    total_durations.append(0)
                    total_counter.append(0)
                    #At stop = state 1
            prev_station = s.Code
        else:

            t.atStop = 0
            t.station = 0
            t.state = 0
            t.station_LAT = -1
            t.station_LON = -1
            prev_station = 0

    if len(stations_list)==0:
               #print "Line "+str(cur_id) + " never stopped"
        print("Line "+str(cur_id) + " never stopped")
    else:
        try:
                #Checking if there are distinct source dest.
            s_d = find_sources(stations_list,total_counter)
            journeyed.append(cur_id)
            terminals.append(s_d)
        except ValueError:
                pass

    #trip_id = assign_direction(GPS_log,journeyed,terminals, trip_id)
    #print_log(GPS_log)

    trip_id = assign_direction(GPS_log, journeyed, terminals, trip_id)
    print 'yeeay'
    return [GPS_log, trip_id]

def write_to_sql(j_log,cur, db):
    str_temp=str("")
    for r in j_log:
            str_temp = '''INSERT INTO Journey_Log23(trip_ID, Time_Stamp, LAT, LON, first_line, brigade,
            vehicle_ID, prev_time, prev_distance, prev_duration, atStop, station, direction, station_LAT, station_LON)'''
            str_temp = str_temp+' VALUES(\'' +str(r.trip_ID)+'\',\''+str(r.time1)
            str_temp = str_temp +'\',\''+ str(r.LAT)+'\',\''+str(r.LON)+'\',\''+str(r.first_line)+'\',\''+str(r.brigade)
            str_temp = str_temp +'\',\''+str(r.vehicle_ID)+'\',\''+str(r.prev_time)
            str_temp = str_temp +'\',\''+str(r.prev_distance)+'\',\''+str(r.prev_duration)+'\',\''+str(r.atStop)+'\',\''+str(r.station)
            str_temp = str_temp+'\',\''+str(r.direction)+'\',\''+str(r.station_LAT)+'\',\''+str(r.station_LON)+'\')'


            cur.execute(str_temp)
            db.commit()
    return

def create_j_patterns(j_log):

    line_numbers = []
    journey_patterns = []
    journey_visits =[]
    journey_durations =[]
    pattern = []
    visits = []
    durations = []
    cur_line = j_log[0].first_line
    prev_station = -1
    for r in j_log:
        #Per line, not brigade
        if r.first_line!=cur_line:
            #New line has started
            line_numbers.append(cur_line)

            journey_patterns.append(pattern)
            journey_durations.append(durations)
            journey_visits.append(visits)

            cur_lone = r.first_line
            pattern = []
            visits = []
            durations = []

            pattern.append(r.station)
            visits.append(0)
            durations.append(0)
            prev_station = r.station
        if prev_station!=r.station:
            prev_station = r.station


            if r.station in pattern:

                pattern.append(r.station)
                visits.append(0)
                durations.append(0)




db = conn_MySQL()
cur = db.cursor()

    # Use all the SQL you like
cur.execute("SELECT Time1, LAT, LON, first_line, brigade FROM table1 WHERE LAT>52 and LAT<54 and LON<23 and LON > 19 AND first_line=23 ORDER BY ABS(first_line), ABS(brigade), Time1")
raw_data = cur.fetchall()
v_id_list = []
v_id_recs = []
max_num_recs = 100000
#35 failed
for r in raw_data:
    v_id = str(int(r[3]))+'_'+str(int(r[4]))
    try:
        ind = v_id_list.index(v_id)
        v_id_recs[ind]+=1
    except ValueError:
        v_id_list.append(v_id)
        v_id_recs.append(0)
temp_sum =0
v_indices = []
list_ind = []
for k,v in enumerate(v_id_list):
    temp_sum+= v_id_recs[k]
    if temp_sum>=max_num_recs:
        v_indices.append(list_ind)

        list_ind = []
        temp_sum = 0
        list_ind.append(v)
    else:
        list_ind.append(v)
    # print all the first cell of all the rows
v_indices.append(list_ind)

bus_stops = []

cur.execute("SELECT * FROM stops")
for row in cur.fetchall():
            #LAT, LON, Name, Code
            #Assuming 00000 code is out.
            Rec = namedtuple('BUS_Record', 'LON, LAT, Name, Code')
            Rec.LON = row[0]
            Rec.LAT = row[1]
            Rec.Name = row[2]
            Rec.Code = row[3]
            bus_stops.append(Rec)



cur.execute("SELECT Time1, LAT, LON, first_line, brigade, ID FROM table1 WHERE LAT>52 and LAT<54 and LON<23 and LON > 19 AND first_line=23 ORDER BY ABS(first_line), ABS(brigade), Time1")
raw_data = cur.fetchall()

v_max =27 #max velocity of 100 km/h
max_drift = 20#50 #was 20

#400 000 records tops.
GPS_log = []
cur_vehicle = "-1"
cur_time = datetime.date.today()
cur_LAT = 0
cur_LON = 0
outlier_flag = False
trip_id = 0
for i in range(0,len(v_indices)):

    print "Batch "+str(i) + " out of "+str(len(v_indices))
    for k,row in enumerate(raw_data):
        v_id = str(int(row[3]))+'_'+str(int(row[4]))
        if v_id in v_indices[i]:
            print "Outlier processing: "+str(k)+" out of " +str(len(raw_data))+ ", Vehcile: "+str(v_id)
            #read: time, LAT, LON, first_line,  brigade
            #Todo: consider adding entry time, departure time. Aggregating into a log of stations.
            Rec = namedtuple('GPS_Record', 'trip_ID, time1, LAT, LON, first_line, brigade, vehicle_ID, prev_time, prev_distance, '
                                           'prev_duration, atStop, station, direction, station_LAT, station_LON') #state
            Rec.vehicle_ID = v_id
            Rec.time1 = row[0]
            Rec.LAT = row[1]
            Rec.LON = row[2]
            Rec.first_line = str(int(row[3]))
            Rec.brigade = str(int(row[4]))
            if cur_vehicle!=Rec.vehicle_ID:

                cur_vehicle = Rec.vehicle_ID
                cur_time = Rec.time1
                cur_LAT = Rec.LAT
                cur_LON = Rec.LON
                prev_duration = 0
                prev_distance = 0
                Rec.prev_distance = prev_distance
                Rec.prev_duration = prev_duration
            else:
                    Rec.prev_time = cur_time
                    Rec.prev_duration = (Rec.time1 - cur_time).total_seconds()
                    point1 = (Rec.LAT, Rec.LON)
                    point2 = (cur_LAT, cur_LON)
                    #Distance in meters.
                    Rec.prev_distance =haversine(point1,point2)*1000

                    cur_vehicle = Rec.vehicle_ID
                    cur_time = Rec.time1
                    cur_LAT = Rec.LAT
                    cur_LON = Rec.LON


            if Rec.prev_duration>0 and Rec.prev_distance/Rec.prev_duration > v_max:
                        outlier_flag = True
                        #Traveling without moving
            elif Rec.prev_duration ==0 and Rec.prev_distance > max_drift:
                        outlier_flag = True
                        #Duplicate recording
            if Rec.prev_duration ==0 and Rec.prev_distance==0:
                        outlier_flag = True

            if outlier_flag==False:
                GPS_log.append(Rec)
                #print GPS_log[len(GPS_log)-1].vehicle_ID
            else:
                outlier_flag=False

    cur_vehicle = "-1"
    cur_time = datetime.date.today()
    cur_LAT = 0
    cur_LON = 0

    memory = []
    direction = 1
    prev_station=-1

    for r in GPS_log:
        if cur_vehicle!=r.vehicle_ID:
                cur_vehicle = r.vehicle_ID
                cur_time = r.time1
                cur_LAT = r.LAT
                cur_LON = r.LON
                r.prev_distance = 0
                r.prev_duration = 0
                #trip_id+=1
                #direction =1
                #r.trip_ID = trip_id

                #Assumption: all brigades start from same place - problematic (todo)
                #r.direction = direction

        else:
                r.prev_time = cur_time
                r.prev_duration = (r.time1 - cur_time).total_seconds()
                point1 = (r.LAT, r.LON)
                point2 = (cur_LAT, cur_LON)
                #Distance in meters.
                r.prev_distance =haversine(point1,point2)*1000
                cur_vehicle = r.vehicle_ID
                cur_time = r.time1
                cur_LAT = r.LAT
                cur_LON = r.LON

        s = atStop(r,bus_stops)
        #is at stop

        if s.Code !=-1:

            r.atStop = 1
            r.station = s.Code
            r.state = 1
            r.station_LAT = s.LAT
            r.station_LON = s.LON

        else:

            r.atStop = 0
            r.station = 0
            r.state = 0
            r.station_LAT = -1
            r.station_LON = -1
        #Detect return


        # if s.Code in memory and s.Code!=prev_station and len(memory)>0:
        #     #Found a return
        #     memory = []
        #     trip_id+=1
        #     r.trip_ID = trip_id
        #     if direction==1:
        #         direction =2
        #     else:
        #         direction=1
        #     r.direction = direction
        #     prev_station = s.Code
        # elif s.Code!=-1:
        #     memory.append(s.Code)
        #     r.trip_ID = trip_id
        #     r.direction = direction
        #     prev_station = s.Code
        # else:
        #     r.trip_ID = trip_id
        #     r.direction = direction

        #print r
    #print ("Log size:" + str(len(GPS_log)))


    #[j_log,trip_id] = create_journey_log(GPS_log, bus_stops, trip_id)
    write_to_sql(GPS_log,cur,db)
    GPS_log =[]
    outlier_flag = False
    prev_duration = 0
    prev_distance = 0

db.close()