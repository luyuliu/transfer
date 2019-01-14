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
db_realtime = client.cota_real_time
db_history = client.cota_transfer

db_time_stamps_set = set()
db_time_stamps = []
raw_stamps = db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw = int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()


def find_gtfs_time_stamp(today_date, single_date):
    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


def convertSeconds(BTimeString, single_date):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds + int((single_date - date(1970, 1, 1)).total_seconds()) + \
        18000  # +18000 is the time zone.

# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def sortQuery(A):
    return A["seq"]


a_start = time.time()


def paralleling_transfers(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    that_time_stamp = find_gtfs_time_stamp(today_date, single_date)
    today_weekday = single_date.weekday()

    db_seq = db_GTFS[str(that_time_stamp) + "_trip_seq"]
    db_today_collection = db_history[today_date]
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    db_dedicated_collection = client.cota_dedicated[today_date]

    # The count of current dedicated collection
    count_ded_col = db_dedicated_collection.count()

    # data retrival
    # the scheduled transfers for today
    db_validated_transfer = list(
        db_today_collection.find({}))  # today's transfer
    db_realtime_collection = db_realtime["R" + today_date]
    print(today_date)

    Normal_count = 0
    Missed_count = 0
    None_count = 0
    Preemptive_count = 0
    Critical_count = 0
    Max_count = len(db_validated_transfer)
    # True to always wipe out when starting unless the count is exactly the same. False to switch to default.
    toSkip = False

    if count_ded_col == Max_count and toSkip == True:
        print("[", single_date, "]: Done.")
        return False
    else:
        db_dedicated_collection.drop()
        print("[", single_date, "]: Drop.")

    Total_count = 0
    Ded_count = 0
    records_collections = []

    dedicated_route_id = 2

    for single_result in db_validated_transfer:
        a = time.time()

        # If the status is normal/missed/preemptive, consider change it.
        if single_result["status"] < 3 or single_result["status"] == 6:
            single_result["nor_b_a_t"] = single_result["b_a_t"]
            single_result["nor_b_a_seq"] = single_result["b_a_seq"]
            single_result["nor_b_a_tr"] = single_result["b_a_tr"]

            # reassess_flag to true, which means the revisit of transfers will change this specific transfer.
            reassess_flag = True

            flag = 0
            # If the generating trip is the dedicated bus route.
            if single_result["a_ro"] == dedicated_route_id or single_result["a_ro"] == -dedicated_route_id:
                flag = "a"
                # Revise a_real_time to a_time, which change the actual time to schedule time

                single_result["a_r_t"] = single_result["a_t"]

                false_trips_list = list(db_seq.find({"service_id": str(
                    service_id), "stop_id": single_result["b_st"], "route_id": single_result["b_ro"]}))  # Find all possible receiving bus according to the b_stop_id and b_route_id and service_id
                # Sort the query results according to the seq. This is a full list of possible trips.
                false_trips_list.sort(key=sortQuery)
                b_a_tr = "0"  # b_alternative_trip_id
                b_a_seq = ""  # b_alternative_seq_id = ATP's N
                b_a_real_seq = 0
                b_a_t = 9999999999  # b_alternative_time
                # These three are the information of the actual receiving bus.

                seq_query = list(db_seq.find({"service_id": str(
                    service_id), "stop_id": single_result["b_st"], "trip_id": single_result["b_tr"]}))  # To find the scheduled receiving bus's seq_id, query the b_trip_id in the trip_seq collection.
                if seq_query == []:  # If there is no such a records, the scheduled receiving bus is not in the trip_seq, which is theoretical impossible.
                    b_a_t == -2

                else:
                    # Select the first one in the seq_query (which can only have one.)
                    seq_query = seq_query[0]
                    # This is the seq_id of the scheduled receiving bus.
                    flag_sequence_id = seq_query["seq"]

                    # For all the possible receiving buses, find the closest one as the actual receiving bus.
                    for each_trip in false_trips_list:
                        i_trip_id = each_trip["trip_id"]
                        seq_id = each_trip["seq"]
                        # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
                        # print(b_stop_id,str(i_trip_id))
                        query_realtime = list(db_realtime_collection.find(
                            {"stop_id": single_result["b_st"], "trip_id": str(i_trip_id)}))  # Find each trip's real arrival time.
                        if query_realtime == []:  # Cannot find the real time.
                            # Which suggests that the bus is gone. We won't take care of it but to let it go till we find an existing bus.
                            pass
                            # This is tricky
                        else:
                            i_real_time = query_realtime[0]["time"]
                            if b_a_t > i_real_time and i_real_time >= single_result["a_r_t"] + single_result["w_t"]:
                                b_a_t = i_real_time

                                # store the closest trip's seq_id.
                                b_a_real_seq = seq_id
                                b_a_seq = seq_id - flag_sequence_id
                                b_a_tr = i_trip_id

                # This means there's no alternative trip for this receiving trip. So you are doomed.
                if b_a_t == 9999999999:
                    # print(single_result["b_st"],single_result["b_tr"])
                    b_a_t = -1  # there's no an alternative trip.
                    b_a_seq = None
                    b_a_tr = "-1"
                # Become a missing record, which is pretty weird.
                if b_a_t == -2:
                    b_a_seq = "Missed"
                    b_a_tr = "-2"

                #print(single_result["b_st"], single_result["a_ro"], "=>", single_result["b_ro"], "|| ", single_result["a_r_t"]-,  b_a_t,
                #      "ded: ", b_a_real_seq, "schedule: ", flag_sequence_id, "nor: ", single_result["nor_b_a_seq"]+flag_sequence_id)

            # If the receiving trip is the dedicated bus route.
            elif single_result["b_ro"] == dedicated_route_id or single_result["b_ro"] == -dedicated_route_id:
                # Which means we need to query from the GTFS static data.
                flag = "b"
                real_b_seq = list(db_seq.find({"service_id": str(service_id), "stop_id": single_result["b_st"], "route_id": single_result["b_ro"], "time": {
                                  "$gte": single_result["a_r_t"] + single_result["w_t"] - 18000 - int((single_date - date(1970, 1, 1)).total_seconds())}}).sort([("seq", 1)]))
                if real_b_seq==[]: # There is no possible 
                    print("???")
                else:
                    real_b_seq=real_b_seq[0]
                    # print(real_b_seq)
                    b_a_t_real = real_b_seq["time"] + \
                        int((single_date - date(1970, 1, 1)).total_seconds()) + 18000
                    b_a_seq_real = real_b_seq["seq"]
                    b_a_tr_real = real_b_seq["trip_id"]

                    schedule_b_seq = list(db_seq.find({"service_id": str(
                        service_id), "stop_id": single_result["b_st"], "trip_id": single_result["b_tr"]}))[0]

                    b_a_seq_schedule = schedule_b_seq["seq"]

                    b_a_t = b_a_t_real
                    b_a_seq = b_a_seq_real - b_a_seq_schedule
                    b_a_tr = b_a_tr_real
            
            # If not assessed, then reassess_flag is False.
            else:
                reassess_flag = False

            if reassess_flag == True:
                single_result["b_a_t"] = b_a_t
                single_result["b_a_seq"] = b_a_seq
                single_result["b_a_tr"] = b_a_tr
                temp_status = single_result["status"]
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
                    print(single_result)
                    single_result["status"] = 5
                    None_count += 1

                # print(temp_status,"=>",single_result["status"],flag )

                Ded_count += 1
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
                    None_count += 1
        else:
            None_count += 1

        records_collections.append(single_result)
        Total_count += 1
        b = time.time()
        # if Total_count % 1000 ==50:
        if Total_count % 1000 == 50:
            print("[", single_date, "]: ", Total_count, "||", Normal_count, "|", Missed_count, "|", Preemptive_count, "|", Critical_count, "|", None_count, "||",
                  round((Normal_count + Missed_count + Preemptive_count + Critical_count) / Total_count, 4), "||", round(Total_count / Max_count * 100, 4), "%", "||", Ded_count, "||", round(b - a_start, 2))

        if Total_count % 10000 == 9999:
            ass = time.time()
            db_dedicated_collection.insert_many(records_collections)
            records_collections = []
            bss = time.time()
            print("Insert", Total_count / Max_count)

    db_dedicated_collection.insert_many(records_collections)
    print("Insert", Total_count / Max_count)
    return True


if __name__ == '__main__':
    start_date = date(2018, 1, 29)
    end_date = date(2018, 3, 20)

    '''
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(paralleling_transfers, date_range)
    pool.close()
    pool.join()'''

    paralleling_transfers(start_date)
    '''
    for each_date in daterange(start_date, end_date):
        paralleling_transfers(each_date)
'''
