baseLocation="I:\\OSU\\Transfer"
dataLocation=baseLocation+"\\data"
import csv, json, math
from pymongo import MongoClient
from datetime import timedelta, date

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs_1
db_transfer=db_GTFS.transfer

db_feed= client.cota_tripupdate
db_tripupdate=db_feed.trips
db_real_transfer=db_feed.transfer

# date setup
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2017, 11, 29)
end_date = date(2018, 1, 3)


#main loop
for single_date in daterange(start_date, end_date):# enumerate every day in the range
    today_date=single_date.strftime("%Y%m%d")#date
    today_weekday=single_date.weekday()#day of week
    if today_weekday<5:
        service_id=1
    elif today_weekday==5:
        service_id=2
    else:
        service_id=3

    #data retrival
    db_schedule=db_transfer.find({"b_sid":str(service_id)})# the scheduled transfers for today
    
    print(today_date)
    for each_transfer in db_schedule:
        
        b_trip_id=each_transfer["b_tid"]
        a_trip_id=each_transfer["a_tid"]
        walking_time=each_transfer["w_time"]
        b_stop_id=each_transfer["b_stid"]
        a_stop_id=each_transfer["a_stid"]
        real_transfer={}
        real_transfer["a_stid"]=a_stop_id
        real_transfer["a_tid"]=a_trip_id
        real_transfer["b_stid"]=b_stop_id
        real_transfer["b_tid"]=b_trip_id
        real_transfer["w_time"]=walking_time
        
        a_real_time=-1
        print(int(today_date),int(b_trip_id))
        db_real_b_trip=list(db_tripupdate.find({"start_date":int(today_date),"trip_id":int(b_trip_id)}))
        db_real_a_trip=list(db_tripupdate.find({"start_date":int(today_date),"trip_id":int(a_trip_id)}))
        
        if db_real_a_trip==[] or db_real_b_trip==[]:
            #no record found!
            real_transfer["diff"]=None
            real_transfer["status"]=None
            db_real_transfer.insert(real_transfer)
            ####################################################
            continue

        #print(db_real_b_trip)
        nearest_sequence_id=99999
        b_real_time=-1
        print(list(db_real_b_trip))
        for each_b_trip in db_real_b_trip:
            #print(each_b_trip)
            b_seq=json.loads(each_b_trip["seq"])
            flag=False
            for each_b_seq in b_seq:
                if each_b_seq["stop"]==b_stop_id:
                    nearest_sequence_id=each_b_seq["seq"]
                    b_real_time=each_b_seq["arr"]
                    flag=True
                    break
            if flag==False:# when flag is false, it means no target stop detected. It also means the bus already passed the bus.
                break;
            print(nearest_sequence_id)



