baseLocation="I:\\OSU\\Transfer"
dataLocation=baseLocation+"\\data"
import csv, json, math
from pymongo import MongoClient
from datetime import timedelta, date

# database setup
client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs_1
db_trip=db_GTFS.trips

db_feed= client.cota_tripupdate
db_tripupdate=db_feed.trips

# date setup
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)


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
    db_schedule=db_trip.find({"service_id":str(service_id)})# the scheduled transfers for today
    
    print(today_date)
    progress_count=0
    total_count=0
    for each_trip in db_schedule:
        total_count+=1
        b_trip_id=str(each_trip["trip_id"])
        db_real_b_trip=list(db_tripupdate.find({"start_date":today_date,"trip_id":b_trip_id}))
        #print(today_date,b_trip_id,progress_count)
        if db_real_b_trip==[]:
            progress_count=progress_count+1
            #print(int(today_date),int(b_trip_id))
    print(today_date,progress_count,total_count)