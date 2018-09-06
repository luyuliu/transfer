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
db_GTFS = client.cota_gtfs_1
db_seq=db_GTFS.trip_seq
db_stops=db_GTFS.stops
db_stop_times=db_GTFS.stop_times
db_trips=db_GTFS.trips
db_tripupdate=client.cota_tripupdate

db_realtime=client.cota_real_time

# date setup


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


start_date = date(2018, 1, 29)
end_date = date(2018, 2, 28)

time_matrix={}

# main loop
# enumerate every day in the range
for single_date in daterange(start_date, end_date):
    start_time=time.time()
    today_date = single_date.strftime("%Y%m%d")  # date
    db_feed_collection=db_tripupdate[today_date]

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
    