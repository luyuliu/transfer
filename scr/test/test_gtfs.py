import math
import json
import csv
import datetime
from datetime import date, timedelta
from pymongo import MongoClient
baseLocation = "I:\\OSU\\Transfer"
dataLocation = baseLocation+"\\data"

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs
db_trip = db_GTFS.trips

db_feed = client.cota_tripupdate
db_tripupdate = db_feed.trips


# date setup
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

db_time_stamps_set = set()
db_time_stamps = []
raw_stamps = db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw = int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()

def find_gtfs_time_stamp(single_date):
    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


start_date = date(2018, 1, 29)
end_date = date(2018, 9, 3)


# main loop
# enumerate every day in the range
b=0
for single_date in daterange(start_date, end_date):
    that_time_stamp = find_gtfs_time_stamp(single_date)
    a=(datetime.datetime.utcfromtimestamp(that_time_stamp).strftime('%Y-%m-%d %H:%M:%S'))
    if that_time_stamp!=b:
        print(a)
    b=that_time_stamp