
import sys
import os
from pymongo import MongoClient
from datetime import timedelta, date
import datetime, time
import multiprocessing
client = MongoClient('mongodb://localhost:27017/')

sys.path.append(os.path.dirname((os.path.dirname(os.path.abspath(__file__)))))
import transfer_tools

from itertools import chain

db_GTFS = client.cota_gtfs
db_real_time = client.cota_real_time
db_trip_update = client.trip_update
db_smart_transit = client.cota_apc_pr_opt
db_ar = client.cota_ar

db_diff = db_ar
db_diff_reduce = client.cota_diff_reduce

walking_time_limit = 10  # min
criteria = 0  # seconds
designated_route_id = 2


def reduce_diff(start_date, end_date):
    date_range = transfer_tools.daterange(start_date, end_date)

    wt_list = [0] *24
    wt_list_count = [0] *24
    for single_date in date_range:

        today_date = single_date.strftime("%Y%m%d")  # date
        col_diff = db_diff[today_date]
        today_seconds = time.mktime(time.strptime(today_date, "%Y%m%d"))

        rl_opt_result = list(
            col_diff.find({}))
            
        for each_record in rl_opt_result:
            time_alt = each_record["time"] # ar
            time_arr = each_record["rand_time"]

            try:
                time_arr = int(time_arr)
            except:
                continue

            # time_alt = each_record["time_actual"] # nr
            # time_arr = each_record["time_normal"]

            # time_alt = each_record["time_er_alt"] # er
            # time_arr = each_record["time_er_arr"]
            
            # time_alt = each_record["time_er_alt"] # er
            # time_arr = each_record["time_er_arr"]

            if type(time_alt) is int and type(time_arr) is not str and time_alt != 0 and time_arr != 0:
                diff_seconds = int((time_alt - today_seconds)/3600)
                if diff_seconds>23:
                    diff_seconds = 23
                
                # print(diff_seconds)
                if time_alt - time_arr < 0:
                    continue
                wt_list[diff_seconds] += time_alt - time_arr
                wt_list_count[diff_seconds] += 1

        # for each_record in rl_opt_result:
        #     for i in range(10):
        #         try:
        #             # time_alt = each_record["time_alt_" + str(i)]
        #             # time_arr = each_record["time_smart_" + str(i)]

        #             time_alt = each_record["time_rr_alt_" + str(i)]
        #             time_arr = each_record["time_rr_arr_" + str(i)]
        #             diff_seconds = int((time_alt - today_seconds)/3600)
        #             if diff_seconds>23:
        #                 diff_seconds = 23
        #         except:
        #             continue

        #         if type(time_alt) is int and type(time_arr) is int and time_alt != 0 and time_arr != 0:
        #             wt_list[diff_seconds] += time_alt - time_arr
        #             wt_list_count[diff_seconds] += 1
        
        print(today_date,end =" ")
        for i in range (24):
            print(wt_list[i],end =" ")

        print("")
        
    for i in range(24):
        if wt_list_count[i] != 0:
            wt_list[i] = (wt_list[i]/wt_list_count[i])

    print(today_date,end =" ")
    for i in range (24):
        print(wt_list[i],end =" ")
    print("")
    print(today_date,end =" ")
    for i in range (24):
        print(wt_list_count[i],end =" ")
    print("")


if __name__ == "__main__":
    start_date = date(2018, 2, 1)
    end_date = date(2019, 1, 31)
    reduce_diff(start_date, end_date)

    # Todo: find skip reason
