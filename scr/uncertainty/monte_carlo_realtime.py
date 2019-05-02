import pymongo
from datetime import timedelta, date
import time
import multiprocessing

single_date = date(2018, 2, 1)
today_date = single_date.strftime("%Y%m%d")  # date
monte_carlo_range = 60

# Database setup
client = pymongo.MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs
db_tripupdate = client.trip_update
db_realtime = client.cota_real_time
db_monte_realtime = client.monte_real_time
col_monte_realtime = db_monte_realtime[today_date +
                                       "_" + str(monte_carlo_range)]

db_time_stamps_set = set()
db_time_stamps = []
raw_stamps = db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw = int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()


# Functions
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


# Find the corresponding GTFS set.
def find_gtfs_time_stamp(today_date, single_date):
    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


def convertSeconds(BTimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    return hours * 3600 + minutes * 60 + seconds


all_realtime = (db_realtime.find({}))
monte_realtime_list = []

for single_realtime in all_realtime:
    single_realtime["time"] = single_realtime["time"] * (1 + )