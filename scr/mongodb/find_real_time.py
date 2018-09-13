import pymongo
from datetime import timedelta, date
import time

def convertSeconds(BTimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds

# database setup
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs

db_tripupdate=client.trip_update

db_realtime=client.cota_real_time

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()


# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def find_gtfs_time_stamp(today_date,single_date):
    today_seconds=int((single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup=db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds> 86400:
            return backup
        backup=each_time_stamp
    return db_time_stamps[len(db_time_stamps)-1]


start_date = date(2018, 1, 29)
end_date = date(2018, 9, 3)

time_matrix={}

# main loop
# enumerate every day in the range
for single_date in daterange(start_date, end_date):
    start_time=time.time()
    today_date = single_date.strftime("%Y%m%d")  # date
    db_feed_collection=db_tripupdate[today_date]
    print("-----------------------",today_date,"-----------------------")

    that_time_stamp=find_gtfs_time_stamp(today_date,single_date)
        
    db_seq=db_GTFS[str(that_time_stamp)+"_trip_seq"]
    db_stops=db_GTFS[str(that_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(that_time_stamp)+"_stop_times"]
    db_trips=db_GTFS[str(that_time_stamp)+"_trips"]

    db_realtime_collection=db_realtime["R"+today_date]
    db_feeds=list(db_feed_collection.find({}))
    print("-----------------------","FindDone","-----------------------")
    total_count=len(db_feeds)
    count=0
    for each_feed in db_feeds:
        trip_id=each_feed["trip_id"]
        seq_count=0
        for each_stop in each_feed["seq"]:
            stop_id=each_stop["stop"]
            try:
                time_matrix[trip_id]
            except:
                time_matrix[trip_id]={}
            else:
                pass
            try:
                time_matrix[trip_id][stop_id]
            except:
                time_matrix[trip_id][stop_id]={}
                time_matrix[trip_id][stop_id]={"seq":seq_count,"time":each_stop["arr"]}
            else:
                if time_matrix[trip_id][stop_id]["seq"]>seq_count:
                    time_matrix[trip_id][stop_id]={"seq":seq_count,"time":each_stop["arr"]}
            seq_count+=1
        
        if count%10000==0:
            print("-----------------------","QueryDoneBy:",count/total_count*100,"-----------------------")
        count+=1
    
    for trip_id in time_matrix.keys():
        for stop_id in time_matrix[trip_id].keys():
            recordss={}
            recordss["trip_id"]=trip_id
            recordss["stop_id"]=stop_id
            recordss["seq"]=time_matrix[trip_id][stop_id]["seq"]
            recordss["time"]=time_matrix[trip_id][stop_id]["time"]
            db_realtime_collection.insert_one(recordss)
    print("-----------------------","InsertDone:","-----------------------")
    end_time=time.time()
    print(end_time-start_time)
    db_realtime_collection.create_index([("trip_id",1),("stop_id",1)])
    