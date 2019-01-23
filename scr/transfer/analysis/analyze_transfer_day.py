import shapefile
from pymongo import MongoClient
from datetime import timedelta, date
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def switch_status(status, line, hour):
    if status == 0:
        line['zero_c_' + str(hour)] += 1
    elif status == 1:
        line['one_c_' + str(hour)] += 1
    elif status == 2:
        line['two_c_' + str(hour)] += 1
    elif status == 6:
        line['crit_c_' + str(hour)] += 1
    else:
        line['miss_c_' + str(hour)] += 1
    line['totl_c_' + str(hour)] += 1


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

db_history = client.cota_transfer

# main loop
# enumerate every day in the range


def analyze_transfer(single_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    that_time_stamp = find_gtfs_time_stamp(today_date, single_date)
    today_first_seconds = int(
        (single_date - date(1970, 1, 1)).total_seconds()) + 18000
    print(that_time_stamp)

    db_today_collection = db_history[today_date]
    db_stops = db_GTFS[str(that_time_stamp) + "_stops"]

    db_result_collection = []
    db_result = list(db_today_collection.find(
        {"b_a_t": {"$lte": today_first_seconds + 21600}}))
    db_result_collection.append(db_result)
    db_result = list(db_today_collection.find(
        {"b_a_t": {"$gt": today_first_seconds + 21600, "$lte": today_first_seconds + 43200}}))
    db_result_collection.append(db_result)
    db_result = list(db_today_collection.find(
        {"b_a_t": {"$gt": today_first_seconds + 43200, "$lte": today_first_seconds + 64800}}))
    db_result_collection.append(db_result)
    db_result = list(db_today_collection.find(
        {"b_a_t": {"$gt": today_first_seconds + 64800}}))
    db_result_collection.append(db_result)

    # b_a_t is the time we use to decide whether the transfer's category

    dic_stops = {}

    # print(db_result)
    for hour in range(4):
        print(len(db_result_collection[hour]))
        for single_result in db_result_collection[hour]:
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
                dic_stops[a_stop_id] = line

            try:
                dic_stops[a_stop_id]["totl_TTP_" + str(hour)]
            except:
                dic_stops[a_stop_id]["totl_TTP_" + str(hour)] = 0
                dic_stops[a_stop_id]['totl_c_' + str(hour)] = 0
                dic_stops[a_stop_id]['zero_c_' + str(hour)] = 0
                dic_stops[a_stop_id]['one_c_' + str(hour)] = 0
                dic_stops[a_stop_id]['two_c_' + str(hour)] = 0
                dic_stops[a_stop_id]['miss_c_' + str(hour)] = 0
                dic_stops[a_stop_id]['crit_c_' + str(hour)] = 0

            switch_status(single_result['status'], dic_stops[a_stop_id], hour)
            if single_result['status'] < 3:
                single_TTP = single_result['b_a_t'] - single_result['b_t']
                # print(single_TTP)
                dic_stops[a_stop_id]["totl_TTP_" + str(hour)] += single_TTP

    # print(dic_stops)
    location = 'D:/Luyu/transfer_data/hour_shp/' + today_date + "_nor.shp"
    # print(location)
    w = shapefile.Writer(location)
    w.field("stop_id", "C")

    for hour in range(4):
        w.field("totl_c_" + str(hour), "N")
        '''
        w.field("zero_c_"+str(hour), "N")
        w.field("one_c_"+str(hour), "N")
        w.field("two_c_"+str(hour), "N")
        w.field("miss_c_"+str(hour), "N")
        w.field("crit_c_"+str(hour), "N")'''

        w.field("totl_TTP_" + str(hour), "F")
        w.field("ave_TTP_" + str(hour), "F")
        w.field("tran_rsk_" + str(hour), "F")

    for key, value in dic_stops.items():
        ave_TTP = []
        trans_risk = []
        for hour in range(4):

            try:
                value["totl_TTP_" + str(hour)]
            except:
                value["totl_TTP_" + str(hour)] = 0
                value['totl_c_' + str(hour)] = 0
                value['zero_c_' + str(hour)] = 0
                value['one_c_' + str(hour)] = 0
                value['two_c_' + str(hour)] = 0
                value['miss_c_' + str(hour)] = 0
                value['crit_c_' + str(hour)] = 0

            if value['zero_c_' + str(hour)] + value['one_c_' + str(hour)] + value['two_c_' + str(hour)] == 0:
                ave_TTP.append(None)
            else:
                ave_TTP.append(value['totl_TTP_' + str(hour)] / (value['zero_c_' + str(
                    hour)] + value['one_c_' + str(hour)] + value['two_c_' + str(hour)]))

            if value['totl_c_'+str(hour)] == 0:
                trans_risk.append(None)
            else:
                trans_risk.append(value['one_c_'+str(hour)] / value['totl_c_'+str(hour)]*10000)

        w.record(key, value['totl_c_0'], value['totl_TTP_0'], ave_TTP[0], trans_risk[0] , value['totl_c_1'], value['totl_TTP_1'], ave_TTP[1], trans_risk[1] , value['totl_c_2'], value['totl_TTP_2'], ave_TTP[2], trans_risk[2] , value['totl_c_3'], value['totl_TTP_3'], ave_TTP[3], trans_risk[3] )
        w.point(float(value['lon']), float(value['lat']))


if __name__ == '__main__':
    start_date = date(2018, 1, 31)
    end_date = date(2018, 2, 3)
    '''
    cores = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=cores)
    date_range = daterange(start_date, end_date)
    output=[]
    output=pool.map(analyze_transfer, date_range)
    pool.close()
    pool.join()
    '''
    date_range = daterange(start_date, end_date)
    for i in date_range:
        analyze_transfer(i)
    # analyze_transfer(start_date)
