import pymongo
from datetime import timedelta, date
import time

def convertSeconds(BTimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds

def sortArray(a):
    return a["time"]

# database setup
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()





for each_time_stamp in db_time_stamps:
    db_seq=db_GTFS[str(each_time_stamp)+"_trip_seq"]
    db_stops=db_GTFS[str(each_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(each_time_stamp)+"_stop_times"]
    db_trips=db_GTFS[str(each_time_stamp)+"_trips"]

    query_stop_time=db_stop_times.find({},no_cursor_timeout=True)

    A={}
    B=0

    total_seq_stop_time=[]



    print("-----------------------","FindDone","-----------------------")
    for each_stop_time in query_stop_time:
        seq_stop_time={}

        seq_stop_time["stop_id"]=each_stop_time["stop_id"]
        seq_stop_time["trip_id"]=each_stop_time["trip_id"]
        seq_stop_time["time"]=convertSeconds(each_stop_time["arrival_time"])

        trip_info=list(db_trips.find({"trip_id":seq_stop_time["trip_id"]}))[0]
        seq_stop_time["service_id"]=trip_info["service_id"]
        route_id=int(trip_info["route_id"])
        if trip_info["direction_id"]=="0":
            seq_stop_time["route_id"]=route_id
        elif trip_info["direction_id"]=="1":
            seq_stop_time["route_id"]=-route_id
        else:
            print("wrong")
            
        try:
            A[seq_stop_time["service_id"]]
        except:
            A[seq_stop_time["service_id"]]={}
        else:
            pass
        try:
            A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]]
        except:
            A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]]={}
        else:
            pass
        try:
            A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]][seq_stop_time["route_id"]]
        except:
            A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]][seq_stop_time["route_id"]]=[]
        else:
            pass
        A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]][seq_stop_time["route_id"]].append({"trip_id":seq_stop_time["trip_id"],"time":seq_stop_time["time"]})
        A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]][seq_stop_time["route_id"]].sort(key=sortArray)
        total_seq_stop_time.append(seq_stop_time)
        B+=1
        if B%1000==0:
            print("-----------------------",B,"-----------------------")

    print("-----------------------","SearchDone","-----------------------")

    for seq_stop_time in total_seq_stop_time:
        index=A[seq_stop_time["service_id"]][seq_stop_time["stop_id"]][seq_stop_time["route_id"]].index({"trip_id":seq_stop_time["trip_id"],"time":seq_stop_time["time"]})
        seq_stop_time["seq"] = index
        db_seq.insert_one(seq_stop_time)


    db_seq.create_index([("service_id",1),("stop_id",1),("route_id",1),("trip_id",1)])

