baseLocation="I:\\OSU\\Transfer"
dataLocation=baseLocation+"\\data"
import csv, json

nearDic=[]
stopTimesDic=[]


with open(dataLocation+'\\stops.csv', newline='') as stopsFile:
    stopsReader = csv.reader(stopsFile, delimiter=',')
    stopsDicKeys=['FID', 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 'zone_id', 'stop_url', 'location_t', 'parent_sta', 'stop_timez', 'wheelchair']
    #print(stopsDicKeys)
    for row in stopsReader:
        rowDic={}
        for key in range(len(stopsDicKeys)):
            rowDic[stopsDicKeys[key]]=row[key]
        rowDic["stop_trip_id"]=[]#record of each stop's trip id
        stopsDic[row[1]]=(rowDic)


with open(dataLocation+'\\near.csv', newline='') as nearFile:
    nearReader = csv.reader(nearFile, delimiter=',')
    nearDicKeys=['OBJECTID', 'IN_FID', 'NEAR_FID', 'NEAR_DIST', 'NEAR_RANK']
    #print(nearDicKeys)
    for row in nearReader:
        rowDic={}
        for key in range(len(nearDicKeys)):
            rowDic[nearDicKeys[key]]=row[key]
        nearDic.append(rowDic)

with open(dataLocation+'\\stop_times.csv', newline='') as stopTimesFile:
    stopTimesReader = csv.reader(stopTimesFile, delimiter=',')
    stopTimesDicKeys=['trip_id','arrival_time','departure_time','stop_id','stop_sequence','stop_headsign','pickup_type','drop_off_type','shape_dist_traveled']
    #print(stopTimesDicKeys)
    for row in stopTimesReader:
        rowDic={}
        for key in range(len(stopTimesDicKeys)):
            rowDic[stopTimesDicKeys[key]]=row[key]
        stopTimesDic.append(rowDic)

for stopsTime in stopTimesDic:
    stop_id=stopsTime["stop_id"]
    trip_id=stopsTime["trip_id"]
    stopsDic[stop_id]["stop_trip_id"].append(trip_id)

