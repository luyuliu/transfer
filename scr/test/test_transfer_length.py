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
end_date = date(2018, 9, 3)

db_history=client.cota_transfer

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

    print("Date: ",single_date,"||","Total",db_today_collection.estimated_document_count(),"||","Normal: ",db_today_collection.find({"status":0}).count(),"Missed: ",db_today_collection.find({"status":1}).count(),"Preemptive: ",db_today_collection.find({"status":2}).count())