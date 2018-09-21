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
    that_time_stamp=find_gtfs_time_stamp(today_date,single_date)

    db_seq=db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_today_collection = db_history[today_date]
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3
    db_dedicated_collection= client.db_dedicated[today_date]

    # data retrival
    # the scheduled transfers for today
    db_validated_transfer = list(db_today_collection.find({}))
    db_realtime_collection=db_realtime["R"+today_date]
    print(today_date)

    Normal_count = 0
    Missed_count = 0
    None_count = 0
    Preemptive_count = 0
    Critical_count = 0
    Max_count = len(db_validated_transfer)
    Total_count = 0

    records_collections = []

    dedicated_route_id=2
    
    for single_result in db_validated_transfer:
        a = time.time()
        if single_result["a_ro"]==dedicated_route_id or single_result["a_ro"]==-dedicated_route_id:
            pass
        
        if single_result["b_ro"]==dedicated_route_id or single_result["b_ro"]==-dedicated_route_id:
            schedule_b_seq=list(db_seq.find({"service_id":str(service_id),"route_id":single_result["b_ro"],"time":{"$gte":single_result["b_r_t"]-18000-int((single_date - date(1970, 1, 1)).total_seconds())}}).sort( [("seq",1)] ))[0]



        







        records_collections.append(single_result);
        Total_count += 1
        if Total_count % 1000 ==50:
            print("[",single_date,"]: ",Total_count,"||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "||",
                round((Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count,4),"||", round(Total_count / Max_count * 100, 4), "%"  ,"||",round(b - a,2),"||",round(b - a_start,2))
       
        if Total_count % 10000 == 9999:
            ass = time.time()
            db_today_collection.insert_many(records_collections)
            records_collections = []
            bss = time.time()
            print("insert", bss - ass)

    db_today_collection.insert_many(records_collections)
    return True
 


if __name__ == '__main__':
    start_date = date(2018, 8, 27)
    end_date = date(2018, 8, 28)
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(paralleling_transfers, date_range)
    pool.close()
    pool.join()
