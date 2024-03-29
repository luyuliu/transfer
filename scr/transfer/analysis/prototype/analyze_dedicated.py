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
        line['zero_count'] += 1
    elif status == 1:
        line['one_count'] += 1
    elif status == 2:
        line['two_count'] += 1
    elif status == 6:
        line['critical_count'] += 1
    else:
        line['miss_count'] += 1

    line['total_count'] += 1


db_time_stamps_set = set()
db_time_stamps = []
raw_stamps = db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw = int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()


def find_gtfs_time_stamp(today_date, single_date):
    today_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    backup = db_time_stamps[0]
    for each_time_stamp in db_time_stamps:
        if each_time_stamp - today_seconds > 76400:
            return backup
        backup = each_time_stamp
    return db_time_stamps[len(db_time_stamps) - 1]


'''start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)'''

db_history = client.cota_dedicated

# main loop
# enumerate every day in the range


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    that_time_stamp = find_gtfs_time_stamp(today_date, single_date)

    db_today_collection = db_history[today_date]
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]

    db_result = list(db_today_collection.find({}))

    dic_stops = {}

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
            line["total_TTP"] = 0
            line['total_count'] = 0
            line['zero_count'] = 0
            line['one_count'] = 0
            line['two_count'] = 0
            line['miss_count'] = 0
            line['critical_count'] = 0

            dic_stops[a_stop_id] = line

        switch_status(single_result['status'], dic_stops[a_stop_id])
        if single_result['status'] < 3:
            single_TTP = single_result['b_a_t'] - single_result['b_t']
            # print(single_TTP)
            dic_stops[a_stop_id]["total_TTP"] += single_TTP

    location = 'D:/Luyu/transfer_data/ded_shp/' + today_date+'_ded.shp'
    # print(location)
    w = shapefile.Writer(location)
    w.field("stop_id", "C")
    w.field("total_count", "N")
    w.field("zero_count", "N")
    w.field("one_count", "N")
    w.field("two_count", "N")
    w.field("miss_count", "N")
    w.field("critical_count", "N")

    w.field("total_TTP", "F")
    w.field("ave_TTP", "F")
    w.field("tran_risk", "F")
    for key, value in dic_stops.items():
        if value['zero_count']+value['one_count']+value['two_count']==0:
            continue
        else:
            ave_TTP=value['total_TTP']/(value['zero_count']+value['one_count']+value['two_count'])

        if value['total_count']==0:
            continue
        else:
            trans_risk=value['one_count']/value['total_count']

        w.record(key, value['total_count'], value['zero_count'],
                 value['one_count'], value['two_count'], value['miss_count'], value['critical_count'],value['total_TTP'], ave_TTP,trans_risk*100)
        
        w.point(float(value['lon']), float(value['lat']))


if __name__ == '__main__':
    start_date = date(2018, 1, 29)
    end_date = date(2018, 9, 3)
    '''
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
    '''
    analyze_transfer(start_date)
