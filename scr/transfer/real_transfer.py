import csv, json, math
from pymongo import MongoClient
from datetime import timedelta, date

client = MongoClient('mongodb://localhost:27017/')
db_real_transfer = client.cota_real_transfer


current_collection=db_real_transfer["Jan"]
aggregated_collection=db_real_transfer["aggregated"]

received_transfers=list(current_collection.find({"transaction_type":118}))

real_transfers=[]
summary_transfer=[]


for each_transfer in received_transfers:
    single_transfer={}

    single_transfer['b_route_id']=each_transfer["route"]
    single_transfer['b_timestamp']=each_transfer["timestamp"]
    single_transfer['b_stop_code']=each_transfer["stop_name"]
    single_transfer['b_bus_id']=each_transfer["bus"]
    single_transfer['a_bus_id']=each_transfer["orig_bus"]
    single_transfer['identifier']=each_transfer["n"]

    issued_transfer=list(current_collection.find({"transaction_type":128,"bus":each_transfer["orig_bus"],"n":each_transfer["n"]}))[0]
    single_transfer['a_route_id']=issued_transfer["route"]
    single_transfer['a_timestamp']=each_transfer["timestamp"]
    single_transfer['a_stop_id']=each_transfer["stop_name"]
    real_transfers.append(single_transfer)

    try:
        summary_transfer

