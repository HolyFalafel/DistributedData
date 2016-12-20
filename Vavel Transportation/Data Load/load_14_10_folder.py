# import ..\VaVEL
import MySQLdb
import csv
import os

def conn_MySQL():

    db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                         user="root",         # your username
                         passwd="Ar050783",  # your password
                         db="vavel-warsaw")        # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need

    return db


db = conn_MySQL()
cur = db.cursor()

root = "c:\\"
dir = "Users\danny\Google Drive\VaVeL\\2016-10-14\\2016-10-14"
fulldir = root + dir


directory = os.path.join(root, dir)


# columns
FileName = 0
Brigade = 1
FirstLine = 2
ReceivedTime = 3
Status = 4
RawLong = 5
RawLat = 6
CleanLong = 7
CleanLat = 8
Lines = 9
LowFloor = 10
TramStatus = 11
DelayedBy_Presumed = 12
NearestStopName_Raw = 13
Time = 14
NearestStopName = 15
NearestStopDistance = 16
NearestStopLong = 17
NearestStopLat = 18
previousStopName = 19
previousStopLong = 20
previousStopLat = 21
previousStopDistance = 22
previousStopArrivalTime = 23
previousStopLeaveTime = 24
nextStopName = 25
nextStopLong = 26
nextStopLat = 27
nextStopDistance = 28
nextStopTimetableVisitTime = 29
courseDirection_Raw_Presumed = 30
courseDirection = 31
timetableIdentifier = 32
timetableStatus = 33
Unknown1 = 34
Unknown2 = 35


num_of_files = 0
total_rows = 0
rows_in_file = 0

for root, dirs, files in os.walk(directory):
    for file in files:
        num_of_files += 1
        # if file.endswith(".csv"):
        with open(fulldir + "\\" + file, 'r') as csvfile:

            FileName_Data = file
            print 'loading file no: ', num_of_files, " name: ", file

            reader = csv.DictReader(csvfile, fieldnames=['Brigade','FirstLine','ReceivedTime','Status',
                                                         'RawLong','RawLat','CleanLong','CleanLat','Lines','LowFloor',
                                                         'TramStatus','DelayedBy_Presumed','NearestStopName_Raw','Time',
                                                         'NearestStopName','NearestStopDistance','NearestStopLong',
                                                         'NearestStopLat','previousStopName','previousStopLong',
                                                         'previousStopLat','previousStopDistance',
                                                         'previousStopArrivalTime','previousStopLeaveTime',
                                                         'nextStopName','nextStopLong','nextStopLat','nextStopDistance',
                                                         'nextStopTimetableVisitTime','courseDirection_Raw_Presumed',
                                                         'courseDirection','timetableIdentifier','timetableStatus',
                                                         'Unknown1','Unknown2'],
                                    delimiter=';')
            rows_in_file = 1
            for row in reader:
                RowNum_Data = rows_in_file
                total_rows += 1
                # get values
                Brigade_Data = row['Brigade']
                FirstLine_Data = row['FirstLine']
                ReceivedTime_Data = row['ReceivedTime']
                Status_Data = row['Status']
                RawLong_Data = row['RawLong']
                RawLat_Data = row['RawLat']
                CleanLong_Data = row['CleanLong']
                CleanLat_Data = row['CleanLat']
                Lines_Data = row['Lines']
                LowFloor_Data = row['LowFloor']
                TramStatus_Data = row['TramStatus']
                DelayedBy_Presumed_Data = row['DelayedBy_Presumed']
                NearestStopName_Raw_Data = row['NearestStopName_Raw']
                Time_Data = row['Time']
                NearestStopName_Data = row['NearestStopName']
                NearestStopDistance_Data = row['NearestStopDistance']
                NearestStopLong_Data = row['NearestStopLong']
                NearestStopLat_Data = row['NearestStopLat']
                previousStopName_Data = row['previousStopName']
                previousStopLong_Data = row['previousStopLong']
                previousStopLat_Data = row['previousStopLat']
                previousStopDistance_Data = row['previousStopDistance']
                previousStopArrivalTime_Data = row['previousStopArrivalTime']
                previousStopLeaveTime_Data = row['previousStopLeaveTime']
                nextStopName_Data = row['nextStopName']
                nextStopLong_Data = row['nextStopLong']
                nextStopLat_Data = row['nextStopLat']
                nextStopDistance_Data = row['nextStopDistance']
                nextStopTimetableVisitTime_Data = row['nextStopTimetableVisitTime']
                courseDirection_Raw_Presumed_Data = row['courseDirection_Raw_Presumed']
                courseDirection_Data = row['courseDirection']
                timetableIdentifier_Data = row['timetableIdentifier']
                timetableStatus_Data = row['timetableStatus']
                Unknown1_Data = row['Unknown1']
                Unknown2_Data = row['Unknown2']

                # Fix data
                if LowFloor_Data == "TRUE":
                    LowFloor_Data = 1
                else:
                    LowFloor_Data = 0

                # empty time
                if previousStopArrivalTime_Data == '':
                    previousStopArrivalTime_Data = None

                if previousStopLeaveTime_Data == '':
                    previousStopLeaveTime_Data = None

                if Unknown2_Data == 'null':
                    Unknown2_Data = None

                # empty lat/long
                if nextStopLong_Data == '':
                    nextStopLong_Data = 0.0

                if nextStopLat_Data == '':
                    nextStopLat_Data = 0.0

                record = (FileName_Data,RowNum_Data,Brigade_Data,FirstLine_Data,ReceivedTime_Data,Status_Data,RawLong_Data,
                          RawLat_Data,CleanLong_Data,CleanLat_Data,Lines_Data,LowFloor_Data,TramStatus_Data,
                          DelayedBy_Presumed_Data,NearestStopName_Raw_Data,Time_Data,NearestStopName_Data,
                          NearestStopDistance_Data,NearestStopLong_Data,NearestStopLat_Data,previousStopName_Data,
                          previousStopLong_Data,previousStopLat_Data,previousStopDistance_Data,
                          previousStopArrivalTime_Data,previousStopLeaveTime_Data,nextStopName_Data,nextStopLong_Data,
                          nextStopLat_Data,nextStopDistance_Data,nextStopTimetableVisitTime_Data,
                          courseDirection_Raw_Presumed_Data,courseDirection_Data,timetableIdentifier_Data,
                          timetableStatus_Data,Unknown1_Data,Unknown2_Data)

                #  insert to db
                # Updating row in DB
                try:
                    # insert data
                    cur.execute("INSERT INTO `vavel-warsaw`.`brigades_data` "
                                "(`FileName`,`RowNum`,`Brigade`,`FirstLine`,`ReceivedTime`,`Status`,`RawLong`,"
                                "`RawLat`,`CleanLong`,`CleanLat`,`Lines`,`LowFloor`,`TramStatus`,"
                                "`DelayedBy_Presumed`,`NearestStopName_Raw`,`Time`,`NearestStopName`,"
                                "`NearestStopDistance`,`NearestStopLong`,`NearestStopLat`,`previousStopName`,"
                                "`previousStopLong`,`previousStopLat`,`previousStopDistance`,`previousStopArrivalTime`,"
                                "`previousStopLeaveTime`,`nextStopName`,`nextStopLong`,`nextStopLat`,"
                                "`nextStopDistance`,`nextStopTimetableVisitTime`,`courseDirection_Raw_Presumed`,"
                                "`courseDirection`,`timetableIdentifier`,`timetableStatus`,`Unknown1`,`Unknown2`) "
                                "VALUES"
                                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); ",
                                # ON DUPLICATE KEY UPDATE # ...
                                record)
                    # # delete
                    # command ='delete from `dogdb`.`Dog_Pup_Of_Dog` where `Tag_No` = '+ Tag_No+';'
                    # myCursor.execute(command)
                except Exception as e:
                    print "* error adding record no.", rows_in_file, "in file: ", file, ". error: ", e, ", record:"
                    print record

                rows_in_file += 1

            # commit after every file
            db.commit()

        csvfile.close()

print "total files opened: ", num_of_files, ", total rows read: ", total_rows