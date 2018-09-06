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
import time

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs_1
db_transfer = db_GTFS.transfer
db_seq = db_GTFS.trip_seq
db_feed = client.cota_tripupdate

db_history = client.cota_transfer


def convertSeconds(BTimeString, single_date):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds + int((single_date - date(1970, 1, 1)).total_seconds()) + \
        18000

# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2018, 1, 31)
end_date = date(2018, 2, 8)
'''
start_date = date(2018, 2, 8)
end_date = date(2018, 2, 25)
'''


def sortQuery(A):
    return A["seq"]


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

    db_today_collection = db_history[today_date]

    db_tripupdate = db_feed[today_date]

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

    records_count = 0
    records_collections = []

    for each_transfer in db_schedule:
        a = time.time()
        print("----------------------------------------------")
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
        real_transfer["a_st"] = a_stop_id
        real_transfer["a_tr"] = a_trip_id
        real_transfer["a_ro"] = each_transfer["a_route_id"]
        real_transfer["a_t"] = each_transfer["a_time"] + \
            int((single_date - date(1970, 1, 1)).total_seconds()) + \
            18000  # time gap

        real_transfer["b_st"] = b_stop_id
        real_transfer["b_tr"] = b_trip_id
        real_transfer["b_ro"] = each_transfer["b_route_id"]
        real_transfer["b_t"] = each_transfer["b_time"] + \
            int((single_date - date(1970, 1, 1)).total_seconds()) + \
            18000  # time gap

        real_transfer["w_t"] = walking_time
        real_transfer["schd_diff"] = each_transfer["b_time"] - \
            each_transfer["a_time"]

        db_real_b_trip = list(db_tripupdate.find(
            {"trip_id": (b_trip_id)}))

        db_real_a_trip = list(db_tripupdate.find(
            {"trip_id": (a_trip_id)}))
        # print((today_date),(b_trip_id),db_real_b_trip)

        ##### Start: Omit the null value #####
        # Find the missing records in the trip update database. No trip_id found.
        if db_real_a_trip == [] or db_real_b_trip == []:  # no record found!
            real_transfer["diff"] = None
            real_transfer["status"] = 5  # "RECORD_MISS"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            print("M: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
                                                                                                                                                            Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue

        # Find the real_record in the feed data according to the tripid and stopid
        b_real_time = -1
        b_trip_feeds = list(db_tripupdate.find(
            {"trip_id": str(b_trip_id), "seq.stop": b_stop_id}))

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
                    b_real_time = b_stop_time["arr"]
                    break

        # If b_real_time=-1, then there are no such a stop detected in the feed.
        if b_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 4  # "STOP_MISS_B"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            print("B: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
                                                                                                                                                            Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue

        a_real_time = -1
        a_trip_feeds = list(db_tripupdate.find(
            {"trip_id": str(a_trip_id), "seq.stop": a_stop_id}))
        if len(a_trip_feeds) != 0:
            for a_stop_time in a_trip_feeds[len(a_trip_feeds) - 1]["seq"]:
                if a_stop_time["stop"] == a_stop_id:
                    a_real_time = a_stop_time["arr"]
                    break
        if a_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 3  # "STOP_MISS_A"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            print("A: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
                                                                                                                                                            Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue
        ##### Done: Omit the null value #####

        ##### Start: Initialization #####
        real_transfer["b_r_t"] = b_real_time
        real_transfer["a_r_t"] = a_real_time
        real_transfer["diff"] = b_real_time - a_real_time
        b_alt_trip_id = "0"
        b_alt_sequence_id = ""
        b_alt_time = 9999999999
        ##### Done: Initialization #####

        ar = time.time()
        ##### Start: Calculate the real receiving trip. And the addtional time penalty #####

        '''
        false_trips_list = list(db_GTFS.trips.find(
            {"route_id": "%03d" % b_route_id, "direction_id": b_direction_id, "service_id": str(service_id)}))  # Find all trips that could be a substitute. AND IT'S FROM THE SCHEDULE.

        i_sequence_id = 0
        flag_sequence_id = 0
        start_time = time.time()
        for each_trip in false_trips_list:  # For each trips, find the real departure time for it, which still satisfies the requirement: the departure time is after
                                            # the user's arrival time (stop A arrival time + walking time).
                                            # Find the relative real time of departure from tripupdate database is not very easy since we have to look up every records
                                            # in the query results, and find the nearest stop. But since in the query result, the sequence is already from the start to destination.
                                            # So just go to the last record, and get the b_alt_time of this record.
                                            # Then find the smallest/closest trip as the alternative trip for the trip B.
            i_trip_id = each_trip["trip_id"]
            stop_time_query = list(db_GTFS.stop_times.find(
                {"trip_id": i_trip_id, "stop_id": b_stop_id}))

            if len(stop_time_query) == 0:
                each_trip["sequence_id"] = None
                continue

            each_trip["sequence_id"] = i_sequence_id
            each_trip["b_time"] = stop_time_query[0]["arrival_time"]
            if i_trip_id == b_trip_id:
                flag_sequence_id = i_sequence_id

            i_sequence_id = i_sequence_id + 1
        end_time = time.time()
        print("queue",end_time - start_time)
        '''

        false_trips_list = list(db_seq.find({"service_id": str(
            service_id), "stop_id": b_stop_id, "route_id": real_transfer["b_ro"]}))
        # print(false_trips_list)
        false_trips_list.sort(key=sortQuery)
        # print(false_trips_list)
        seq_query = list(db_seq.find({"service_id": str(
            service_id), "stop_id": b_stop_id, "trip_id": b_trip_id}))[0]
        if seq_query == []:
            continue
        flag_sequence_id = seq_query["seq"]

        for each_trip in false_trips_list:

            each_b_time = 0
            i_trip_id = each_trip["trip_id"]

            seq_id = each_trip["seq"]
            # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
            # The most accurate feed must be the last one of the query result. (To be proven)

            alternative_b_trip = list(db_tripupdate.find({"trip_id": str(i_trip_id),
                                                          "seq": {"$elemMatch": {"stop": b_stop_id}}}))

            '''print("length:",len(alternative_b_trip))
            for each_feed in alternative_b_trip:
                tag=0
                for each_stop_time in each_feed["seq"]:
                    if each_stop_time["stop"] == b_stop_id:
                        print(each_stop_time["arr"],tag)
                        break
                    tag=tag+1'''
            if len(alternative_b_trip) == 0:
                # print("i_trip_id",i_trip_id,"|","each_schedule",each_trip["time"]+int((single_date - date(1970, 1, 1)).total_seconds()) + \
                # 18000,"each_b_time",each_b_time,"id","|",each_trip["seq"])
                continue
            for b_stop_time in alternative_b_trip[len(alternative_b_trip) - 1]["seq"]:
                if b_stop_time["stop"] == b_stop_id:
                    each_b_time = b_stop_time["arr"]
                if b_stop_time["stop"] == b_stop_id and b_alt_time > b_stop_time["arr"] and b_stop_time["arr"] >= a_real_time + real_transfer["w_t"]:
                    b_alt_time = b_stop_time["arr"]
                    b_alt_sequence_id = seq_id - flag_sequence_id
                    b_alt_trip_id = i_trip_id
                    break
            # print("i_trip_id",i_trip_id,"|","each_schedule",each_trip["time"]+int((single_date - date(1970, 1, 1)).total_seconds()) + \
        # 18000,"each_b_time",each_b_time,"id","|",each_trip["seq"])

        # This means there's no alternative trip for this receiving trip. So you are doomed.
        if b_alt_time == 9999999999:
            b_alt_time = -1  # there's no an alternative trip.
            b_alt_sequence_id = None
            b_alt_trip_id = "-1"

        # print("real:",i_sequence_id,"flag:",flag_sequence_id,"skipped",skipped)
        ##### Done: Calculate the real receiving trip. And the addtional time penalty #####
        br = time.time()
        print("real_trip", br - ar)

        # Instead of using diff and w_time to determine the status, we use ATP's N value.
        # Because the sequence of scheduled bus array is not static. Though theoretically yes.
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
        real_transfer["b_a_t"] = b_alt_time
        # The ATP's N value.
        real_transfer["b_a_seq"] = b_alt_sequence_id
        # The value of alternative trip. Could be "0" (not applicable), "-1" (doomed) or some actual trip id (in string format)
        real_transfer["b_a_tr"] = b_alt_trip_id

        ass = time.time()
        records_collections.append(real_transfer)
        bss = time.time()
        print("append", bss - ass)

        # print(real_transfer["status"])
        # if real_transfer["status"] !=0:
        # print(real_transfer["b_real_time"]-real_transfer["w_time"]-real_transfer["a_real_time"],real_transfer["status"])

        # print("status:",real_transfer["status"])
        # print("b_alt_time", b_alt_time, "b_time", real_transfer["b_t"], "b_real_time", b_real_time,
        #        "a_time", real_transfer["a_t"], "a_real_time", a_real_time, "w_time",real_transfer["w_t"],"b_alt_sequence_id", b_alt_sequence_id, "diff", b_real_time - a_real_time - real_transfer["w_t"],"alt_diff",b_alt_time - a_real_time - real_transfer["w_t"])
        print("V: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|",
              (Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count)
        # print("B:",b_stop_id,b_trip_id,"A:",a_stop_id,a_trip_id)

        b = time.time()
        print("total", b - a)

        if records_count % 10000 == 9999:
            ass = time.time()
            db_today_collection.insert_many(records_collections)
            records_collections = []
            bss = time.time()
            print("insert", bss - ass)
        records_count = records_count + 1

    db_today_collection.insert_many(records_collections)
