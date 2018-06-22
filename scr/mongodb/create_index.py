
import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
collection = client.cota_tripupdate.trips

indexes=[("start_date",pymongo.ASCENDING),("trip_id",pymongo.ASCENDING)]

collection.create_index(indexes)
print(indexes+" Over.")