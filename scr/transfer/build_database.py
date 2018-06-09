baseLocation = "I:\\OSU\\Transfer"
dataLocation = "I:\\OSU\\data\\data\\OpenGTFSData_201802"
import csv
import json
import math
from pymongo import MongoClient
stopsDic = {}
stopTimesDic = {}
tripsDic = {}
shapesDic={}
transferDic = {}

walkingSpeed = 1.4  # meters per second
walkingTime = 71.43  # seconds // In order to limit the number of transfer pair
searchRadius = walkingSpeed * walkingTime

client = MongoClient('mongodb://localhost:27017/')
db = client.cota_gtfs_1

current_collection=db["stops"]
shapesDic=list(current_collection.find({}))

current_collection=db["shapes"]
shapesDic=list(current_collection.find({}))

current_collection=db["trips"]
tripsDic=list(current_collection.find({}))

current_collection=db["stop_times"]
stopTimesDic=list(current_collection.find({}))

