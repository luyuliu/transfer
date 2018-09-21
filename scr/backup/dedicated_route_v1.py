# No database write operation needed.

import csv
import json
import math
import shapefile
from pymongo import MongoClient
from datetime import timedelta, date

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs


db_feed = client.cota_tripupdate
db_tripupdate = db_feed.trips

# date setup

def switch_status(status,line):
    if status==0:
        line['zero_count']+=1
    elif status==1:
        line['one_count']+=1
    elif status==2:
        line['two_count']+=1
    else:
        line['miss_count']+=1
    
    line['total_count']+=1

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def convert_to_seconds(randomString):
    time = randomString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    BTotalSeconds = hours * 3600 + minutes * 60 + seconds
    return BTotalSeconds

'''start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)'''

start_date = date(2018, 1, 29)
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

    db_today_collection = db_history[today_date]

    db_result=list(db_today_collection.find({}))

    dic_stops={}

    #print(db_result)
    for single_result in db_result:
        a_stop_id=single_result['a_stop_id']
        b_stop_id=single_result['b_stop_id']

        if single_result["a_route_id"]==1 or single_result["a_route_id"]==-1: # If the dedicated line is the generating trip, then all the scenario (0,1,2) will be 0 (normal).
            single_result["status"]=0 # (0/1/2 -> 0)
        
        if single_result["b_route_id"]==1 or single_result["b_route_id"]==-1: # If the dedicated line is the receiving trip, then 0 is 0, 2 is 1, and 1 depends.
            if single_result["status"]==2: # (2->1)
                single_result["status"]=1
                single_result["b_alt_trip_id"]="0"
                single_result["b_alt_time"]=9999999999
                false_trips_list = db_GTFS.trips.find({"route_id": "%03d" % 1, "direction_id": str(-1/2*single_result["b_route_id"]+1/2)}) # Find all trips that could be a substitute.
            
            
                for each_trip in false_trips_list: # For each trips, find the real departure time for it, which still satisfies the requirement: the departure time is after 
                                                    # the user's arrival time (stop A arrival time + walking time).
                                                    # Find the relative real time of departure from tripupdate database is not very easy since we have to look up every records
                                                    # in the query results, and find the nearest stop. But since in the query result, the sequence is already from the start to destination.
                                                    # So just go to the last record, and get the b_alt_time of this record. 
                                                    # Then find the smallest/closest trip as the alternative trip for the trip B.
                    i_trip_id = each_trip["trip_id"]
                    
                    # Each trip's time at b_stop
                    each_b_stop_time=convert_to_seconds(list(db_GTFS.stop_times.find({"trip_id": str(i_trip_id),"stop_id":b_stop_id}))[0]["arrival_time"])+int((single_date - date(1970, 1, 1)).total_seconds()) +18000
                    #if len(alternative_b_trip)==0:# bookmark
                    #    continue

                    if each_b_stop_time<single_result["a_real_time"]:
                        continue
                    elif each_b_stop_time<single_result["b_alt_time"]:
                        single_result["b_alt_time"]=each_b_stop_time
                        single_result["b_alt_trip_id"]=i_trip_id

            if single_result["status"]==1: # (1->1, but the real receiving bus will change.)
                # Find the new actual receiving bus.
                    single_result["b_alt_trip_id"]="0"
                    single_result["b_alt_time"]=9999999999
                    false_trips_list = db_GTFS.trips.find({"route_id": "%03d" % 1, "direction_id": str(-1/2*single_result["b_route_id"]+1/2)}) # Find all trips that could be a substitute.
                
                
                    for each_trip in false_trips_list: # For each trips, find the real departure time for it, which still satisfies the requirement: the departure time is after 
                                                        # the user's arrival time (stop A arrival time + walking time).
                                                        # Find the relative real time of departure from tripupdate database is not very easy since we have to look up every records
                                                        # in the query results, and find the nearest stop. But since in the query result, the sequence is already from the start to destination.
                                                        # So just go to the last record, and get the b_alt_time of this record. 
                                                        # Then find the smallest/closest trip as the alternative trip for the trip B.
                        i_trip_id = each_trip["trip_id"]
                        
                        # Each trip's time at b_stop
                        each_b_stop_time=convert_to_seconds(list(db_GTFS.stop_times.find({"trip_id": str(i_trip_id),"stop_id":b_stop_id}))[0]["arrival_time"])+int((single_date - date(1970, 1, 1)).total_seconds()) +18000
                        #if len(alternative_b_trip)==0:# bookmark
                        #    continue

                        if each_b_stop_time<single_result["a_real_time"]:
                            continue
                        elif each_b_stop_time<single_result["b_alt_time"]:
                            single_result["b_alt_time"]=each_b_stop_time
                            single_result["b_alt_trip_id"]=i_trip_id



        try:
            dic_stops[a_stop_id]
        except:
            line={}
            a_stop=db_GTFS.stops.find({"stop_id":a_stop_id})[0]
            line['lat']=a_stop['stop_lat']
            line['lon']=a_stop['stop_lon']
            line['total_count'] = 0
            line['zero_count'] = 0
            line['one_count'] = 0
            line['two_count'] = 0
            line['miss_count'] = 0
            dic_stops[a_stop_id]=line
        
        switch_status(single_result['status'],dic_stops[a_stop_id])



    location='I:/OSU/Transfer_data/ded_shp/'+today_date
    print(location)
    w = shapefile.Writer(shapeType=1)
    w.field("stop_id","C")
    w.field("total_count","N")
    w.field("zero_count","N")
    w.field("one_count","N")
    w.field("two_count","N")
    w.field("miss_count","N")
    for key, value in dic_stops.items():
        w.record(key,value['total_count'],value['zero_count'],value['one_count'],value['two_count'],value['miss_count'])
        w.point(float(value['lon']),float(value['lat']))

    w.save(location)
        