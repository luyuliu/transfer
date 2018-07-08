# Status code:
# 0: true
# 1: false
# 2: false_timelessness
# 3: missing_a
# 4: missing_b
# 5: missing_records


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

# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


'''start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)'''

start_date = date(2018, 2, 5)
end_date = date(2018, 2, 25)

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

    True_count = 0
    False_count = 0
    None_count = 0
    Pseudo_count = 0
    Max_count = len(db_schedule)
    Total_count = 0

    route_pairs = []

    for each_transfer in db_schedule:
        # old_sum=True_count+False_count+None_count
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

        if db_real_a_trip == [] or db_real_b_trip == []:
            # no record found!
            real_transfer["diff"] = None
            real_transfer["status"] = 5  # "RECORD_MISS"
            None_count += 1
            db_today_collection.insert(real_transfer)
            print("M: ", True_count, False_count, Pseudo_count, None_count, (True_count +
                                                                             False_count + Pseudo_count) / (Pseudo_count + True_count + False_count + None_count))
            ####################################################
            continue

        # Find the real_record in the feed data according to the tripid and stopid
        b_nearest_sequence_id = 99999
        b_real_time = -1
        for each_b_trip in db_real_b_trip:
            # print(each_b_trip)
            b_seq = each_b_trip["seq"]
            flag = False
            for each_b_seq in b_seq:
                if each_b_seq["stop"] == b_stop_id:
                    if b_nearest_sequence_id > each_b_seq["seq"]:
                        b_nearest_sequence_id = each_b_seq["seq"]
                        b_real_time = each_b_seq["arr"]
                    flag = True
                    break
            if flag == False:  # when flag is false, it means no target stop detected. It also means the bus already passed the bus.
                break
        if b_real_time == -1:  # If b_real_time=-1, then there are no such a stop detected in the feed.
            real_transfer["diff"] = None
            real_transfer["status"] = 4  # "STOP_MISS_B"
            None_count += 1
            db_today_collection.insert(real_transfer)
            print("B: ", True_count, False_count, Pseudo_count, None_count, (True_count +
                                                                             False_count + Pseudo_count) / (Pseudo_count + True_count + False_count + None_count))
            continue

        a_nearest_sequence_id = 99999
        a_real_time = -1
        for each_a_trip in db_real_a_trip:
            # print(each_a_trip)
            a_seq = each_a_trip["seq"]
            flag = False
            for each_a_seq in a_seq:
                if each_a_seq["stop"] == a_stop_id:
                    if a_nearest_sequence_id > each_a_seq["seq"]:
                        a_nearest_sequence_id = each_a_seq["seq"]
                        a_real_time = each_a_seq["arr"]
                    flag = True
                    break
            if flag == False:  # when flag is false, it means no target stop detected. It also means the bus already passed the bus.
                break
        if a_real_time == -1:
            real_transfer["diff"] = None
            real_transfer["status"] = 3  # "STOP_MISS_A"
            None_count += 1
            db_today_collection.insert(real_transfer)
            print("A: ", True_count, False_count, Pseudo_count, None_count, (True_count +
                                                                             False_count + Pseudo_count) / (Pseudo_count + True_count + False_count + None_count))
            continue

        real_transfer["b_real_time"] = b_real_time
        real_transfer["a_real_time"] = a_real_time
        real_transfer["diff"] = b_real_time - a_real_time
        b_alt_trip_id="0"
        b_alt_time=9999999999

        if real_transfer["diff"] < real_transfer["w_time"]:  # False
            # False # the difference between real time is less than the walking time, which means the user can't catch up
            real_transfer["status"] = 1
            False_count += 1
            # Start: Calculate the real receiving trip. And the addtional time penalty
            if real_transfer["b_route_id"] > 0:
                b_route_id = real_transfer["b_route_id"]
                b_direction_id = "0"
            else:
                b_route_id = -real_transfer["b_route_id"]
                b_direction_id = "1"
            false_trips_list = db_GTFS.trips.find(
                {"route_id": "%03d" % b_route_id, "direction_id": b_direction_id}) # Find all trips that could be a substitute.
            
            
            for each_trip in false_trips_list: # For each trips, find the real departure time for it, which still satisfies the requirement: the departure time is after 
                                                # the user's arrival time (stop A arrival time + walking time).
                                                # Find the relative real time of departure from tripupdate database is not very easy since we have to look up every records
                                                # in the query results, and find the nearest stop. But since in the query result, the sequence is already from the start to destination.
                                                # So just go to the last record, and get the b_alt_time of this record. 
                                                # Then find the smallest/closest trip as the alternative trip for the trip B.
                i_trip_id = each_trip["trip_id"]
                alternative_b_trip = list(db_feed.trips.find({"start_date": str(today_date), "trip_id": str(i_trip_id), "seq": {"$elemMatch": {"stop": b_stop_id, "arr": {"$gt": int(a_real_time)+int(real_transfer["w_time"])}}}}))
                #print("hehe!",len(alternative_b_trip))
                if len(alternative_b_trip)==0:
                    continue
                    
                for b_stop_time in alternative_b_trip[len(alternative_b_trip)-1]["seq"]:
                    if b_stop_time["stop"]==b_stop_id and b_alt_time>b_stop_time["arr"]:
                        b_alt_time=b_stop_time["arr"]
                        b_alt_trip_id=i_trip_id
                        break
            #print(b_alt_time,b_alt_trip_id)
            if b_alt_time==9999999999:# This means there's no alternative trip for this receiving trip. So you are doomed.
                b_alt_time=-1 # there's no an alternative trip.
                b_alt_trip_id="-1"


            # Done: Calculate the real receiving trip. And the addtional time penalty

        else:  # True
            if real_transfer["a_real_time"] + real_transfer["w_time"] > real_transfer["b_time"]:
                # print(real_transfer["a_real_time"],real_transfer["w_time"],real_transfer["b_time"],real_transfer["b_real_time"])
                real_transfer["status"] = 2  # "FALSE_TIMELESSNESS"
                Pseudo_count += 1
            else:
                real_transfer["status"] = 0  # True
                True_count += 1

        if b_alt_time==9999999999:
            b_alt_time=0 # do not need an alternative trip.

        real_transfer["b_alt_time"] = b_alt_time # The value of b_alt_time: a) -1: no alternative trip; b) 0: on-time, do not need an alternative trip; c) some time-stamp: not on-time and there's an alternative trip.
        real_transfer["b_alt_trip_id"] = b_alt_trip_id # The value of alternative trip. Could be "0" (not applicable), "-1" (doomed) or some actual trip id (in string format)

        db_today_collection.insert(real_transfer)
        #print(real_transfer["status"])
        if real_transfer["status"] ==1:
            print("V: ",True_count,False_count,Pseudo_count,None_count,(True_count+False_count+Pseudo_count)/(Pseudo_count+True_count+False_count+None_count),b_alt_time-b_real_time)
