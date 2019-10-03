import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats
from pymongo import MongoClient
from datetime import timedelta, date
import datetime, time
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def switch_status(status, line):
    if status == 0:
        line['zero_c'] += 1
    elif status == 1:
        line['one_c'] += 1
    elif status == 2:
        line['two_c'] += 1
    elif status == 6:
        line['crit_c'] += 1
    else:
        line['miss_c'] += 1
    line['totl_c'] += 1


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
    today_date = single_date.strftime("%Y%m%d")  # date
    today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


db_history = client.cota_merge_transfer

# main loop
# enumerate every day in the range
sns.set(color_codes=True)

def analyze_transfer(start_date, end_date):
    date_range = daterange(start_date, end_date)
    dic_stops = {}
    ttp_list = []
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
        that_time_stamp = find_gtfs_time_stamp(single_date)

        db_today_collection = db_history[today_date]
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]

        db_result = (db_today_collection.find({}))

        # print(db_result)
        total_transfer = 0
        total_TTP = 0
        total_missed_transfer = 0

        for single_result in db_result:
            if single_result['status'] < 3:
                single_TTP = single_result['b_a_t'] - \
                    single_result['b_t']
                if single_TTP>1800 or single_TTP<-1800:
                    continue
                if single_result['status'] == 1:
                    total_missed_transfer = total_missed_transfer+1

                ttp_list.append(single_TTP)
        
        print(today_date, ": done")
    
    sns_plot = sns.distplot(ttp_list)
    sns_plot.figure.savefig("D:\\Luyu\\transfer_data\\pic" + today_date + ".png")

if __name__ == "__main__":
    start_date1 = date(2018, 5, 7)
    end_date1 = date(2019, 1, 31)
    analyze_transfer(start_date1, end_date1)