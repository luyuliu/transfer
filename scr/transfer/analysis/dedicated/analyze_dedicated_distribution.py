import shapefile
from pymongo import MongoClient
from datetime import timedelta, date
import datetime
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
raw_stamps = db_GTFS.list_collection_names()
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


db_history = client.cota_dedicated

dedicated_line = 2

# main loop
# enumerate every day in the range


def analyze_transfer(start_date, end_date):
    date_range = daterange(start_date, end_date)
    dic = {}
    dic[-60] = 0
    for single_date in date_range:
        if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
            summer_time = 0
        else:
            summer_time = 1
        today_date = single_date.strftime("%Y%m%d")  # date
        that_time_stamp = find_gtfs_time_stamp(single_date)

        db_today_collection = db_history[today_date]

        db_result = list(db_today_collection.find({}))

        for single_result in db_result:         
            if single_result['a_ro']*single_result['a_ro']!=4 and single_result['b_ro']*single_result['b_ro']!=4:
                continue
            if single_result['status'] < 3:
                try:
                    single_result['nor_b_a_t']
                except:
                    continue
                if single_result['nor_b_a_t'] == -1:
                    continue
                
                single_TTP = round((single_result['b_a_t'] - \
                    single_result['nor_b_a_t']+3600*summer_time)/60,0)
                
                
                if single_TTP < -60:
                    dic[-60] = dic[-60]+1
                else:
                    try:
                        dic[single_TTP]
                    except:
                        dic[single_TTP] = 0
                    dic[single_TTP] = dic[single_TTP]+1
        
        print(single_date)
        
    for key, value in dic.items():
        print(key, value)
                


                



if __name__ == '__main__':
    date_list = []

    start_date1 = date(2018, 11, 4)
    start_date2 = date(2018, 11, 5)
    end_date1 = date(2019, 1, 31)
    analyze_transfer(start_date1, end_date1)
