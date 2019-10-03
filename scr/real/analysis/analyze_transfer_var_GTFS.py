import shapefile
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


db_history = client.cota_transfer

# main loop
# enumerate every day in the range


def analyze_transfer(start_date, end_date):
    date_range = daterange(start_date, end_date)
    dic_stops = {}
    total_transfer = 0
    total_TTP = 0
    total_missed_transfer = 0
    total_TTP_var = 0
    total_risk_var = 0

    for single_date in date_range:
        if (single_date - date(2018, 3, 10)).total_seconds() <= 0 or (single_date - date(2018, 11, 3)).total_seconds() > 0:
            summer_time = 0
        else:
            summer_time = 1
        today_date = single_date.strftime("%Y%m%d")  # date
        today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
        that_time_stamp = find_gtfs_time_stamp(single_date)

        db_today_collection = db_history[today_date]
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]

        db_result = list(db_today_collection.find({}))

        # print(db_result)

        for single_result in db_result:
            a_stop_id = single_result['a_st']
            try:
                dic_stops[a_stop_id]
            except:
                line = {}
                a_stop = list(db_stops.find({"stop_id": a_stop_id}))
                if a_stop == []:
                    continue
                else:
                    a_stop = a_stop[0]
                line['lat'] = a_stop['stop_lat']
                line['lon'] = a_stop['stop_lon']
                line["totl_TTP"] = 0
                line['totl_c'] = 0
                line['zero_c'] = 0
                line['one_c'] = 0
                line['two_c'] = 0
                line['miss_c'] = 0
                line['crit_c'] = 0
                line['max_TTP'] = 0
                line["totl_var"] = 0
                dic_stops[a_stop_id] = line

            switch_status(single_result['status'], dic_stops[a_stop_id])
            if single_result['status'] < 3:
                single_TTP = single_result['b_a_t'] - \
                    single_result['b_t'] + 3600 * summer_time

                total_transfer = total_transfer+1
                total_TTP = total_TTP + single_TTP
                if single_result['status'] == 1:
                    total_missed_transfer = total_missed_transfer+1

                dic_stops[a_stop_id]["totl_TTP"] += single_TTP
                if single_TTP > dic_stops[a_stop_id]["max_TTP"]:
                    dic_stops[a_stop_id]["max_TTP"] = single_TTP

        
        if total_transfer>0:
            print(today_date, len(dic_stops), total_transfer, round(total_TTP/total_transfer,2), round(total_missed_transfer/total_transfer,4))
        else:
            print(today_date, 0)

    print("Final average: ", len(dic_stops), total_transfer, round(total_TTP/total_transfer,2), round(total_missed_transfer/total_transfer,4))
    average_TTP = total_TTP/total_transfer
    average_risk = total_missed_transfer/total_transfer
    total_transfer = 0
    date_range = daterange(start_date, end_date)
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))
        that_time_stamp = find_gtfs_time_stamp(single_date)

        db_today_collection = db_history[today_date]
        db_result = list(db_today_collection.find({}))
        for single_result in db_result:
            a_stop_id = single_result['a_st']
            if single_result['status'] < 3:
                single_TTP = single_result['b_a_t'] - \
                    single_result['b_t']+ 3600 * summer_time
                
                if single_result['status'] == 1:
                    single_missed = 1
                else:
                    single_missed = 0
                total_transfer+=1
                total_TTP_var += (single_TTP - average_TTP)*(single_TTP - average_TTP)
                total_risk_var += (single_missed - average_risk)*(single_missed - average_risk)
        print(today_date, total_transfer, round((total_TTP_var/total_transfer)**0.5,2), round((total_risk_var/total_transfer)**0.5,4))
    print(today_date, total_transfer, round((total_TTP_var/total_transfer)**0.5,2), round((total_risk_var/total_transfer)**0.5,4))




if __name__ == '__main__':
    date_list = []

    start_date1 = date(2018, 1, 31)
    end_date1 = date(2019, 1, 31)

    analyze_transfer(start_date1, end_date1)
