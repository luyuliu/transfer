# Status code:
# 0: Normal transfers
# 1: Missed transfers
# 2: Preemptive transfers
# 3: missing_a
# 4: missing_b
# 5: missing_records
# 6: Critical transfers

import csv
import json
import math
from pymongo import MongoClient
from datetime import timedelta, date

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs_1
db_transfer = db_GTFS.transfer

db_feed = client.cota_tripupdate
db_tripupdate = db_feed.trips


def convertSeconds(BTimeString,single_date):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds+int((single_date - date(1970, 1, 1)).total_seconds()) + \
            18000

# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2018, 2, 5)
end_date = date(2018, 2, 8)
'''
start_date = date(2018, 2, 8)
end_date = date(2018, 2, 25)
'''
db_history = client.cota_transfer

# main loop
# enumerate every day in the range
for single_date in daterange(start_date, end_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3
    try:
        db_feed[today_date].drop()
    except:
        pass

    db_today_collection = db_history[today_date]

    # data retrival
    # the scheduled transfers for today
    db_schedule = list(db_transfer.find({"b_service_id": str(service_id)}))

    print(today_date)

    Normal_count = 0
    Missed_count = 0
    None_count = 0
    Preemptive_count = 0
    Critical_count = 0
    Max_count = len(db_schedule)
    Total_count = 0

    route_pairs = []

    for each_transfer in db_schedule:
        
        # old_sum=Normal_count+Missed_count+None_count
        Total_count += 1
        if Max_count % 1000 == 0:
            print(round(Total_count / Max_count * 100, 4), "%")

        b_trip_id = each_transfer["b_trip_id"]
        a_trip_id = each_transfer["a_trip_id"]
        walking_time = each_transfer["w_time"]
        b_stop_id = each_transfer["b_stop_id"]
        a_stop_id = each_transfer["a_stop_id"]

        real_transfer = {}
        real_transfer["a_stop_id"] = a_stop_id
        real_transfer["a_trip_id"] = a_trip_id
        real_transfer["a_route_id"] = each_transfer["a_route_id"]
        real_transfer["a_time"] = each_transfer["a_time"] + \
            int((single_date - date(1970, 1, 1)).total_seconds()) + \
            18000  # time gap

        real_transfer["b_stop_id"] = b_stop_id
        real_transfer["b_trip_id"] = b_trip_id
        real_transfer["b_route_id"] = each_transfer["b_route_id"]
        real_transfer["b_time"] = each_transfer["b_time"] + \
            int((single_date - date(1970, 1, 1)).total_seconds()) + \
            18000  # time gap

        real_transfer["w_time"] = walking_time
        real_transfer["scheduled_diff"] = each_transfer["b_time"] - \
            each_transfer["a_time"]

        db_real_b_trip = list(db_tripupdate.find(
            {"start_date": (today_date), "trip_id": (b_trip_id)}))
        db_real_a_trip = list(db_tripupdate.find(
            {"start_date": (today_date), "trip_id": (a_trip_id)}))
        # print((today_date),(b_trip_id),db_real_b_trip)

        ##### Start: Omit the null value #####
        # Find the missing records in the trip update database. No trip_id found.
        if db_real_a_trip == [] or db_real_b_trip == []:  # no record found!
            real_transfer["diff"] = None
            real_transfer["status"] = 5  # "RECORD_MISS"
            None_count += 1
            db_today_collection.insert(real_transfer)
            print("M: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
                                                                                                                                                            Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue

        # Find the real_record in the feed data according to the tripid and stopid
        b_real_time=-1
        b_trip_feeds = list(db_tripupdate.find({"start_date": str(today_date), "trip_id": str(b_trip_id), "seq.stop": b_stop_id}))
        '''                      
        for each_feed in b_trip_feeds:
            tag=0
            for each_stop_time in each_feed["seq"]:
                if each_stop_time["stop"] == b_stop_id:
                    print(each_stop_time["arr"],tag)
                    break
                tag=tag+1
        '''
        if len(b_trip_feeds) != 0:
            for b_stop_time in b_trip_feeds[len(b_trip_feeds) - 1]["seq"]:
                if b_stop_time["stop"] == b_stop_id:
                    b_real_time=b_stop_time["arr"]
                    break
        
        # If b_real_time=-1, then there are no such a stop detected in the feed.
        if b_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 4  # "STOP_MISS_B"
            None_count += 1
            db_today_collection.insert(real_transfer)
            print("B: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
                                                                                                                                                            Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue

        a_real_time = -1
        a_trip_feeds = list(db_tripupdate.find({"start_date": str(today_date), "trip_id": str(a_trip_id), "seq.stop": a_stop_id}))
        if len(a_trip_feeds) != 0:
            for a_stop_time in a_trip_feeds[len(a_trip_feeds) - 1]["seq"]:
                if a_stop_time["stop"] == a_stop_id:
                    a_real_time=a_stop_time["arr"]
                    break
        if a_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 3  # "STOP_MISS_A"
            None_count += 1
            db_today_collection.insert(real_transfer)
            print("A: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
                                                                                                                                                            Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue
        ##### Done: Omit the null value #####

        ##### Start: Initialization #####
        real_transfer["b_real_time"] = b_real_time
        real_transfer["a_real_time"] = a_real_time
        real_transfer["diff"] = b_real_time - a_real_time
        b_alt_trip_id = "0"
        b_alt_sequence_id = ""
        b_alt_time = 9999999999
        ##### Done: Initialization #####

        ##### Start: Calculate the real receiving trip. And the addtional time penalty #####
        if real_transfer["b_route_id"] > 0:
            b_route_id = real_transfer["b_route_id"]
            b_direction_id = "0" 
        else:
            b_route_id = -real_transfer["b_route_id"]
            b_direction_id = "1"

        false_trips_list = list(db_GTFS.trips.find(
            {"route_id": "%03d" % b_route_id, "direction_id": b_direction_id,"service_id":str(service_id)}))  # Find all trips that could be a substitute. AND IT'S FROM THE SCHEDULE.

        i_sequence_id = 0
        flag_sequence_id = 0
        skipped=0
        
        for each_trip in false_trips_list:  # For each trips, find the real departure time for it, which still satisfies the requirement: the departure time is after
                                            # the user's arrival time (stop A arrival time + walking time).
                                            # Find the relative real time of departure from tripupdate database is not very easy since we have to look up every records
                                            # in the query results, and find the nearest stop. But since in the query result, the sequence is already from the start to destination.
                                            # So just go to the last record, and get the b_alt_time of this record.
                                            # Then find the smallest/closest trip as the alternative trip for the trip B.
            i_trip_id = each_trip["trip_id"]
            # start_time=time.time()
            stop_time_query = list(db_GTFS.stop_times.find(
                {"trip_id": i_trip_id, "stop_id": b_stop_id}))
            # end_time=time.time()
            # print(end_time-start_time)

            if len(stop_time_query) == 0:
                each_trip["sequence_id"]=None
                skipped=skipped+1
                continue

            each_trip["sequence_id"] = i_sequence_id
            each_trip["b_time"]=stop_time_query[0]["arrival_time"]
            if i_trip_id == b_trip_id:
                flag_sequence_id = i_sequence_id

            i_sequence_id = i_sequence_id + 1

        for each_trip in false_trips_list:
            each_b_time=0
            i_trip_id = each_trip["trip_id"]
            if each_trip["sequence_id"]==None:
                continue
            # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
            # The most accurate feed must be the last one of the query result. (To be proven)
            # start_time=time.time()
            alternative_b_trip = list(db_feed.trips.find({"start_date": str(today_date), "trip_id": str(i_trip_id),
                                                          "seq": {"$elemMatch": {"stop": b_stop_id}}}))
            # end_time=time.time()
            # print(end_time-start_time)
            '''print("length:",len(alternative_b_trip))
            for each_feed in alternative_b_trip:
                tag=0
                for each_stop_time in each_feed["seq"]:
                    if each_stop_time["stop"] == b_stop_id:
                        print(each_stop_time["arr"],tag)
                        break
                    tag=tag+1'''
            if len(alternative_b_trip) == 0:
                #print("i_trip_id",i_trip_id,"|","each_schedule",convertSeconds(each_trip["b_time"],single_date),"each_b_time",each_b_time,"id","|",each_trip["sequence_id"])
                continue
            for b_stop_time in alternative_b_trip[len(alternative_b_trip) - 1]["seq"]:
                if b_stop_time["stop"] == b_stop_id:
                    each_b_time=b_stop_time["arr"]
                if b_stop_time["stop"] == b_stop_id and b_alt_time > b_stop_time["arr"] and b_stop_time["arr"] >= a_real_time + real_transfer["w_time"]:
                    b_alt_time = b_stop_time["arr"]
                    b_alt_sequence_id = each_trip["sequence_id"] - \
                        flag_sequence_id
                    b_alt_trip_id = i_trip_id
                    break
            #print("i_trip_id",i_trip_id,"|","each_schedule",convertSeconds(each_trip["b_time"],single_date),"each_b_time",each_b_time,"id","|",each_trip["sequence_id"])
        
        # This means there's no alternative trip for this receiving trip. So you are doomed.
        if b_alt_time == 9999999999:
            b_alt_time = -1  # there's no an alternative trip.
            b_alt_sequence_id = None
            b_alt_trip_id = "-1"

        
        ##########print("real:",i_sequence_id,"flag:",flag_sequence_id,"skipped",skipped)
        ##### Done: Calculate the real receiving trip. And the addtional time penalty #####

        if b_alt_sequence_id == None:  # Critical transfers
            real_transfer["status"] = 6
            Critical_count += 1
        elif b_alt_sequence_id > 0:  # Missed transfers
            real_transfer["status"] = 1
            Missed_count += 1
        elif b_alt_sequence_id == 0:  # Normal transfers
            real_transfer["status"] = 0
            Normal_count += 1
        elif b_alt_sequence_id < 0:  # Preemptive transfers
            real_transfer["status"] = 2
            Preemptive_count += 1
        else:
            print("Wrong.")

        
        # Collectively assign b_alt_time and b_alt_trip_id for both scenario.
        # The value of b_alt_time: a) -1: no alternative trip; b) 0: on-time, do not need an alternative trip; c) some time-stamp: not on-time and there's an alternative trip.
        real_transfer["b_alt_time"] = b_alt_time
        # The ATP's N value.
        real_transfer["b_alt_sequence_id"] = b_alt_sequence_id
        # The value of alternative trip. Could be "0" (not applicable), "-1" (doomed) or some actual trip id (in string format)
        real_transfer["b_alt_trip_id"] = b_alt_trip_id

        # db_today_collection.insert(real_transfer)
        # print(real_transfer["status"])
        # if real_transfer["status"] !=0:
        # print(real_transfer["b_real_time"]-real_transfer["w_time"]-real_transfer["a_real_time"],real_transfer["status"])

        a = real_transfer["diff"] - real_transfer["w_time"]
        if a >= 0:
            b = 0
        elif a < 0:
            b = 1
        '''
        print("real: ",b_alt_time,b_alt_trip_id,)
        print("schd:",real_transfer["b_real_time"],real_transfer["b_trip_id"],real_transfer["b_time"])
        if real_transfer["status"] !=b:
            print("status:",real_transfer["status"],"||","real_status:",b)
            print("b_alt_time", b_alt_time, "b_time", real_transfer["b_time"], "b_real_time", b_real_time,
                      "a_time", real_transfer["a_time"], "a_real_time", a_real_time, "w_time",real_transfer["w_time"],"b_alt_sequence_id", b_alt_sequence_id, "diff", b_real_time - a_real_time - real_transfer["w_time"],"alt_diff",b_alt_time - a_real_time - real_transfer["w_time"])
            print("V: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|",
                (Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count, "|", b)
            print("B:",b_stop_id,b_trip_id,"A:",a_stop_id,a_trip_id)            
            print("----------------------------------------------")'''
        if real_transfer["status"] != 2:
            if real_transfer["status"] - b != 0:
                print("status:",real_transfer["status"],"||","real_status:",b)
                print("b_alt_time", b_alt_time, "b_time", real_transfer["b_time"], "b_real_time", b_real_time,
                        "a_time", real_transfer["a_time"], "a_real_time", a_real_time, "w_time",real_transfer["w_time"],"b_alt_sequence_id", b_alt_sequence_id, "diff", b_real_time - a_real_time - real_transfer["w_time"],"alt_diff",b_alt_time - a_real_time - real_transfer["w_time"])
                print("V: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|",
                    (Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count, "|", b)
                print("B:",b_stop_id,b_trip_id,"A:",a_stop_id,a_trip_id)         
                print("----------------------------------------------")
