import pymongo
from datetime import timedelta, date
import time

client = pymongo.MongoClient('mongodb://localhost:27017/')
db_feed = client.trip_update
db_tripupdate = db_feed.trip_update

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2018, 1, 29)
end_date = date(2018, 9, 4)

for single_date in daterange(start_date, end_date):
    today_date = single_date.strftime("%Y%m%d")  # date

    db_today_feeds=list(db_tripupdate.find({"start_date": str(today_date)}))
    print("---------------",today_date,": Query","---------------")
    db_feed[today_date].insert_many(db_today_feeds)
    print("---------------",today_date,": Insert","---------------")
    db_feed[today_date].create_index([("trip_id",pymongo.ASCENDING)])
    print("---------------",today_date,": Index","---------------")