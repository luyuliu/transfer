import shapefile
from pymongo import MongoClient
from datetime import timedelta, date
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
    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 86400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


db_history = client.cota_transfer

# main loop
# enumerate every day in the range


def analyze_transfer(start_date, end_date, hour):
    date_range = daterange(start_date, end_date)
    dic_stops = {}
    for single_date in date_range:
        today_date = single_date.strftime("%Y%m%d")  # date
        today_weekday = single_date.weekday()  # day of week

        that_time_stamp = find_gtfs_time_stamp(single_date)

        db_today_collection = db_history[today_date]
        db_stops = db_GTFS[str(that_time_stamp) + "_stops"]

        today_first_seconds = int(
            (single_date - date(1970, 1, 1)).total_seconds()) + 18000

        if hour == 0:
            db_result = list(db_today_collection.find(
                {"b_a_t": {"$lte": today_first_seconds + (hour+1)*60*60}}))
        elif hour == 23:
            db_result = list(db_today_collection.find({"b_a_t": {
                "$gt": today_first_seconds + hour*60*60}}))
        else:
            db_result = list(db_today_collection.find({"b_a_t": {
                "$gt": today_first_seconds + hour*60*60, "$lte": today_first_seconds + (hour+1)*60*60}}))

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
                single_TTP = single_result['b_a_t'] - single_result['b_t']
                # print(single_TTP)
                dic_stops[a_stop_id]["totl_TTP"] += single_TTP
                if single_TTP > line["max_TTP"]:
                    line["max_TTP"] = single_TTP

        for single_result in db_result:
            a_stop_id = single_result['a_st']
            if single_result['status'] < 3:
                single_TTP = single_result['b_a_t'] - single_result['b_t']
                dic_stops[a_stop_id]["totl_var"] += (float(single_TTP - (dic_stops[a_stop_id]["totl_TTP"]/(
                    dic_stops[a_stop_id]['zero_c']+dic_stops[a_stop_id]['one_c']+dic_stops[a_stop_id]['two_c']))) / 60)**2

        print(today_date, len(dic_stops))

    location = 'D:/Luyu/transfer_data/hour_average/' + str(hour) + ".shp"
    print(location)
    w = shapefile.Writer(location)
    w.field("stop_id", "C")
    w.field("totl_c", "N")
    w.field("zero_c", "N")
    w.field("one_c", "N")
    w.field("two_c", "N")
    w.field("miss_c", "N")
    w.field("crit_c", "N")

    w.field("totl_TTP", "N", size=8, decimal=2)
    w.field("ave_TTP", "N", size=8, decimal=2)
    w.field("tran_rsk", "N", size=8, decimal=2)
    w.field("max_TTP", "N", size=8, decimal=2)
    w.field("var", "N", size=8, decimal=2)

    for key, value in dic_stops.items():
        if value['zero_c']+value['one_c']+value['two_c'] == 0:
            continue
        else:
            ave_TTP = float(
                value['totl_TTP']/(value['zero_c']+value['one_c']+value['two_c']))
            var = float(
                value['totl_var']/(value['zero_c']+value['one_c']+value['two_c']))

        if value['totl_c'] == 0:
            continue
        else:
            trans_risk = float(value['one_c'])/float(value['totl_c'])

        w.record(key, value['totl_c'], value['zero_c'],
                 value['one_c'], value['two_c'], value['miss_c'], value['crit_c'], value['totl_TTP'], float(ave_TTP/60), (trans_risk), float(value['max_TTP'])/60, var**0.5)
        w.point(float(value['lon']), float(value['lat']))


if __name__ == '__main__':
    start_date = date(2018, 1, 31)
    end_date = date(2018, 2, 26)
    '''
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
    '''
    '''
    date_range = daterange(start_date, end_date)
    for i in date_range:
        analyze_transfer(i)
'''
    for i in range(24):
        analyze_transfer(start_date, end_date, i)
