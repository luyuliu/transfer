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
db_realtime=client.cota_apc_real_time
db_history = client.cota_merge_transfer

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()

def find_gtfs_time_stamp(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def sortQuery(A):
    return A["seq"]


a_start=time.time()


def paralleling_transfers(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
    that_time_stamp = find_gtfs_time_stamp(single_date)
    today_weekday = single_date.weekday()

    db_seq=db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_today_collection = db_history[today_date]
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3
    db_dedicated_collection= client.cota_merge_dedicated[today_date]

    # data retrival
    # the scheduled transfers for today
    db_validated_transfer = list(db_today_collection.find({}))
    db_realtime_collection=db_realtime[today_date]
    print(today_date)

    Normal_count = 0
    Missed_count = 0
    None_count = 0
    Preemptive_count = 0
    Critical_count = 0
    Max_count = len(db_validated_transfer)
    Total_count = 0
    Ded_count=0
    records_collections = []

    dedicated_route_id=2
    
    for single_result in db_validated_transfer:
        a = time.time()
        
        if single_result["status"]<3 or single_result["status"]==6:
            single_result["nor_b_a_t"]=single_result["b_a_t"]
            single_result["nor_b_a_seq"]=single_result["b_a_seq"]
            single_result["nor_b_a_tr"]=single_result["b_a_tr"]

            reassess_flag=True
            if single_result["a_ro"]==dedicated_route_id or single_result["a_ro"]==-dedicated_route_id:
                single_result["a_r_t"]=single_result["a_t"]

                false_trips_list = list(db_seq.find({"service_id": str(service_id), "stop_id": single_result["b_st"], "route_id": single_result["b_ro"]}))
                # print(false_trips_list)
                false_trips_list.sort(key=sortQuery)
                # print(false_trips_list)
                b_a_tr = "0"
                b_a_seq = ""
                b_a_t = 9999999999

                seq_query = list(db_seq.find({"service_id": str(
                    service_id), "stop_id": single_result["b_st"], "trip_id": single_result["b_st"]}))
                if seq_query == []:
                    continue
                seq_query=seq_query[0]
                flag_sequence_id = seq_query["seq"]

                for each_trip in false_trips_list:
                    i_trip_id = each_trip["trip_id"]
                    seq_id = each_trip["seq"]
                    # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
                    #print(b_stop_id,str(i_trip_id))
                    query_realtime=list(db_realtime_collection.find({"stop_id":single_result["b_st"],"trip_id":str(i_trip_id)}))
                    if query_realtime==[]:
                        continue
                    i_real_time=query_realtime[0]["time"]
                    if b_a_t > i_real_time and i_real_time >= single_result["a_r_t"] + single_result["w_t"]:
                        b_a_t = i_real_time
                        b_a_seq = seq_id - flag_sequence_id
                        b_a_tr = i_trip_id
            
                # This means there's no alternative trip for this receiving trip. So you are doomed.
                if b_a_t == 9999999999:
                    b_a_t = -1  # there's no an alternative trip.
                    b_a_seq = None
                    b_a_tr = "-1"

            elif single_result["b_ro"]==dedicated_route_id or single_result["b_ro"]==-dedicated_route_id:
                real_b_seq=list(db_seq.find({"service_id":str(service_id),"stop_id":single_result["b_st"],"route_id":single_result["b_ro"],"time":{"$gte":single_result["a_r_t"]+single_result["w_t"] - today_seconds}}).sort( [("seq",1)] ))
                if real_b_seq == None or real_b_seq == []:
                    continue
                else:
                    real_b_seq = real_b_seq[0]
                b_a_t_real=real_b_seq["time"]+ today_seconds
                b_a_seq_real=real_b_seq["seq"]
                b_a_tr_real=real_b_seq["trip_id"]

                schedule_b_seq=db_seq.find_one({"service_id":str(service_id),"stop_id":single_result["b_st"],"trip_id":single_result["b_tr"]})

                b_a_seq_schedule=schedule_b_seq["seq"]

                b_a_t=b_a_t_real
                b_a_seq=b_a_seq_real-b_a_seq_schedule
                b_a_tr=b_a_tr_real
            else:
                reassess_flag=False

            
            if reassess_flag==True:
                single_result["b_a_t"]=b_a_t
                single_result["b_a_seq"]=b_a_seq
                single_result["b_a_tr"]=b_a_tr
                if b_a_seq == None:  # Critical transfers
                    single_result["status"] = 6
                    Critical_count += 1
                elif b_a_seq > 0:  # Missed transfers
                    single_result["status"] = 1
                    Missed_count += 1
                elif b_a_seq == 0:  # Normal transfers
                    single_result["status"] = 0
                    Normal_count += 1
                elif b_a_seq < 0:  # Preemptive transfers
                    single_result["status"] = 2
                    Preemptive_count += 1
                else:
                    print("Wrong.")
                
                Ded_count+=1
            else:
                if single_result["status"] == 6:
                    Critical_count += 1
                elif single_result["status"] == 1:
                    Missed_count += 1
                elif single_result["status"] == 0:
                    Normal_count += 1
                elif single_result["status"] == 2:
                    Preemptive_count += 1
                else:
                    print("Wrong.")
        else:
            None_count+=1

        single_result.pop("_id", None)
        records_collections.append(single_result)
        Total_count += 1
        b=time.time()
        if Total_count % 50000 ==50:
            print("[",single_date,"]: ",Total_count,"||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "||",
                    round((Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count,4),"||", round(Total_count / Max_count * 100, 4), "%"  ,"||",Ded_count,"||",round(b - a_start,2))
        
        if Total_count % 10000 == 9999:
            ass = time.time()
            db_dedicated_collection.insert_many(records_collections)
            records_collections = []
            bss = time.time()
            print("insert", bss - ass)

    if records_collections != []:
        db_dedicated_collection.insert_many(records_collections)
    return True
 


if __name__ == '__main__':
    start_date = date(2018, 5, 7)
    end_date = date(2019, 1, 31)
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores-5)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(paralleling_transfers, date_range)
    pool.close()
    pool.join()

    # start_date = date(2018, 9, 4)
    # paralleling_transfers(start_date)