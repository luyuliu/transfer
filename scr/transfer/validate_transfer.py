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

import multiprocessing

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs
db_realtime=client.cota_real_time
db_history = client.cota_transfer

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()

def find_gtfs_time_stamp(today_date,single_date):
    today_seconds=int((single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup=db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds> 86400:
            return backup
        backup=each_time_stamp
    return db_time_stamps[len(db_time_stamps)-1]

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

def sortQuery(A):
    return A["seq"]


a_start=time.time()

def paralleling_transfers(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    that_time_stamp=find_gtfs_time_stamp(today_date,single_date)
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    db_transfer = db_GTFS[str(that_time_stamp)+"_transfers"]
    db_seq=db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_today_collection = db_history[today_date]

    db_history_records=list(db_today_collection.find({}))

    

    # data retrival
    # the scheduled transfers for today
    db_schedule = list(db_transfer.find({"b_service_id": str(service_id)}))

    if len(db_history_records)==len(db_schedule):
        print("[",single_date,"]: Done.")
        return "Skip"
    else: 
        db_today_collection.drop()
        print("[",single_date,"]: Drop.")




    db_realtime_collection=db_realtime["R"+today_date]
    print(today_date)

    Normal_count = 0
    Missed_count = 0
    None_count = 0
    Preemptive_count = 0
    Critical_count = 0
    Max_count = len(db_schedule)
    Total_count = 0

    records_collections = []
    for each_transfer in db_schedule:
        a = time.time()
        # old_sum=Normal_count+Missed_count+None_count
        Total_count += 1

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

        db_real_b_trip = list(db_realtime_collection.find(
            {"trip_id": (b_trip_id)}))

        db_real_a_trip = list(db_realtime_collection.find(
            {"trip_id": (a_trip_id)}))
        # print((today_date),(b_trip_id),db_real_b_trip)

        ##### Start: Omit the null value #####
        # Find the missing records in the trip update database. No trip_id found.
        if db_real_a_trip == [] or db_real_b_trip == []:  # no record found!
            real_transfer["diff"] = None
            real_transfer["status"] = 5  # "RECORD_MISS"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            #print("M: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
            #                                                                                                                                                Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue

        # Find the real_record in the feed data according to the tripid and stopid
        b_real_time = -1
        b_trip_feeds = list(db_realtime_collection.find(
            {"trip_id": str(b_trip_id), "stop_id": b_stop_id}))

        if len(b_trip_feeds) != 0:
            b_real_time = b_trip_feeds[0]["time"]

        # If b_real_time=-1, then there are no such a stop detected in the feed.
        if b_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 4  # "STOP_MISS_B"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            #print("B: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
            #                                                                                                                                                Missed_count + Preemptive_count + Critical_count) / Total_count)
            continue

        a_real_time = -1
        a_trip_feeds = list(db_realtime_collection.find(
            {"trip_id": str(a_trip_id), "stop_id": a_stop_id}))
        if len(a_trip_feeds) != 0:
            a_real_time = a_trip_feeds[0]["time"]
                    
        if a_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 3  # "STOP_MISS_A"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            #print("A: ", real_transfer["status"], "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "|", (Normal_count +
            #                                                                                                                                                Missed_count + Preemptive_count + Critical_count) / Total_count)
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

        ##### Start: Calculate the real receiving trip. And the addtional time penalty #####
        false_trips_list = list(db_seq.find({"service_id": str(
            service_id), "stop_id": b_stop_id, "route_id": real_transfer["b_ro"]}))
        # print(false_trips_list)

        if len(false_trips_list)==0:
            real_transfer["diff"] = None
            real_transfer["status"] = 4  # "STOP_MISS_B"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            continue

        false_trips_list.sort(key=sortQuery)
        # print(false_trips_list)
        seq_query = list(db_seq.find({"service_id": str(
            service_id), "stop_id": b_stop_id, "trip_id": b_trip_id}))
        if len(seq_query) == 0:
            real_transfer["diff"] = None
            real_transfer["status"] = 4  # "STOP_MISS_B"
            None_count += 1
            db_today_collection.insert_one(real_transfer)
            continue
        
        flag_sequence_id = seq_query[0]["seq"]

        for each_trip in false_trips_list:
            i_trip_id = each_trip["trip_id"]
            seq_id = each_trip["seq"]
            # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
            #print(b_stop_id,str(i_trip_id))
            query_realtime=list(db_realtime_collection.find({"stop_id":b_stop_id,"trip_id":str(i_trip_id)}))
            if query_realtime==[]:
                continue
            i_real_time=query_realtime[0]["time"]
            if b_alt_time > i_real_time and i_real_time >= a_real_time + real_transfer["w_t"]:
                b_alt_time = i_real_time
                b_alt_sequence_id = seq_id - flag_sequence_id
                b_alt_trip_id = i_trip_id
            
        # This means there's no alternative trip for this receiving trip. So you are doomed.
        if b_alt_time == 9999999999:
            b_alt_time = -1  # there's no an alternative trip.
            b_alt_sequence_id = None
            b_alt_trip_id = "-1"

        # print("real:",i_sequence_id,"flag:",flag_sequence_id,"skipped",skipped)
        ##### Done: Calculate the real receiving trip. And the addtional time penalty #####
       

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

        records_collections.append(real_transfer)

        # print(real_transfer["status"])
        # if real_transfer["status"] !=0:
        # print(real_transfer["b_real_time"]-real_transfer["w_time"]-real_transfer["a_real_time"],real_transfer["status"])
        b = time.time()
        #print("status:",real_transfer["status"])
        #print("b_alt_time", b_alt_time, "b_time", real_transfer["b_t"], "b_real_time", b_real_time,
        #        "a_time", real_transfer["a_t"], "a_real_time", a_real_time, "w_time",real_transfer["w_t"],"b_alt_sequence_id", b_alt_sequence_id, "diff", b_real_time - a_real_time - real_transfer["w_t"],"alt_diff",b_alt_time - a_real_time - real_transfer["w_t"])
        if Total_count % 1000 ==50:
            print("[",single_date,"]: ",Total_count,"||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "||",
                round((Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count,4),"||", round(Total_count/Max_count,4)  ,"||",round(b - a,2),"||",round(b - a_start,2))
            #print("B:",b_stop_id,b_trip_id,"A:",a_stop_id,a_trip_id)


        if Total_count % 10000 == 9999:
            ass = time.time()
            db_today_collection.insert_many(records_collections)
            records_collections = []
            bss = time.time()
            print("Database Insert:",today_date,"||",Total_count)

    db_today_collection.insert_many(records_collections)
    print("[",single_date,"]: Done.")
    return True
 


if __name__ == '__main__':
    start_date = date(2018, 9, 3)
    end_date = date(2019, 1, 31)


    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=20)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(paralleling_transfers, date_range)
    pool.close()
    pool.join()

    f=open("D:\\Luyu\\transfer_data\\archive\\"+str(time.time())+".txt","a")
    i=0
    for single_date in date_range:
        f.write(str(output[i])+single_date.strftime("%Y%m%d"))
    f.close()


# May 06 not working.