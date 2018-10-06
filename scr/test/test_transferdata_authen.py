baseLocation="I:\\OSU\\Transfer"
dataLocation=baseLocation+"\\data"
import csv, json, math
from pymongo import MongoClient
from datetime import timedelta, date

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs
db_transfer=db_GTFS.transfer

db_feed= client.cota_tripupdate
db_tripupdate=db_feed.trips

# date setup
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

'''start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)'''

start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)

db_history=client.cota_history_transfer

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
    try:
        db_feed[today_date].drop()
    except:
        pass

    db_today_collection=db_history[today_date]

    #data retrival
    db_schedule=db_transfer.find({"b_service_id":str(service_id)})# the scheduled transfers for today
    
    print(today_date)
    
    True_count=0
    False_count=0
    None_count=0

    for each_transfer in db_schedule:
        #old_sum=True_count+False_count+None_count

        b_trip_id=each_transfer["b_trip_id"]
        a_trip_id=each_transfer["a_trip_id"]
        walking_time=each_transfer["w_time"]
        b_stop_id=each_transfer["b_stop_id"]
        a_stop_id=each_transfer["a_stop_id"]

        real_transfer={}
        real_transfer["a_stop_id"]=a_stop_id
        real_transfer["a_trip_id"]=a_trip_id
        real_transfer["a_time"]=each_transfer["a_time"]

        real_transfer["b_stop_id"]=b_stop_id
        real_transfer["b_trip_id"]=b_trip_id
        real_transfer["b_time"]=each_transfer["b_time"]
        real_transfer["w_time"]=walking_time
        
        db_real_b_trip=list(db_tripupdate.find({"start_date":(today_date),"trip_id":(b_trip_id)}))
        db_real_a_trip=list(db_tripupdate.find({"start_date":(today_date),"trip_id":(a_trip_id)}))
        
        if db_real_a_trip==[] or db_real_b_trip==[]:
            #no record found!
            real_transfer["diff"]=None
            real_transfer["status"]=None
            None_count+=1
            #db_today_collection.insert(real_transfer)
            print("!!!!")
            ####################################################
            continue


        b_nearest_sequence_id=99999
        b_real_time=-1
        for each_b_trip in db_real_b_trip:
            #print(each_b_trip)
            b_seq=each_b_trip["seq"]
            flag=False
            for each_b_seq in b_seq:
                if each_b_seq["stop"]==b_stop_id:
                    if b_nearest_sequence_id>each_b_seq["seq"]:
                        b_nearest_sequence_id=each_b_seq["seq"]
                        b_real_time=each_b_seq["arr"]
                    flag=True
                    break
            if flag==False:# when flag is false, it means no target stop detected. It also means the bus already passed the bus.
                break;
        if b_real_time==-1:# If b_real_time=-1, then there are no such a stop detected in the 
            real_transfer["diff"]=None
            real_transfer["status"]=None
            None_count+=1
            #db_today_collection.insert(real_transfer)
            print(True_count,False_count,None_count,(True_count+False_count)/(True_count+False_count+None_count))
            continue;

        '''if(db_real_a_trip==[]):
            print((today_date),(a_trip_id),db_real_a_trip)
        else:
            print((today_date),(a_trip_id),"valid!")'''