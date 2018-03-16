baseLocation="I:\\OSU\\Transfer"
dataLocation="I:\\OSU\\data\\data\\OpenGTFSData_1022"
import csv, json, math
from pymongo import MongoClient
stopsDic={}
stopTimesDic={}
tripsDic={}
transferDic={}

walkingSpeed=1.4#meters per second
walkingTime=5*60#seconds
searchRadius=walkingSpeed*walkingTime

class Haversine:
    '''
    use the haversine class to calculate the distance between
    two lon/lat coordnate pairs.
    output distance available in kilometers, meters, miles, and feet.
    example usage: Haversine([lon1,lat1],[lon2,lat2]).feet
    
    '''
    def __init__(self,coord1,coord2):
        lon1,lat1=coord1
        lon2,lat2=coord2
        lon1=float(lon1)
        lon2=float(lon2)
        lat1=float(lat1)
        lat2=float(lat2)
        
        R=6371000                               # radius of Earth in meters
        phi_1=math.radians(lat1)
        phi_2=math.radians(lat2)

        delta_phi=math.radians(lat2-lat1)
        delta_lambda=math.radians(lon2-lon1)

        a=math.sin(delta_phi/2.0)**2+\
           math.cos(phi_1)*math.cos(phi_2)*\
           math.sin(delta_lambda/2.0)**2
        c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))
        
        self.meters=R*c                         # output distance in meters
        self.km=self.meters/1000.0              # output distance in kilometers
        self.miles=self.meters*0.000621371      # output distance in miles
        self.feet=self.miles*5280               # output distance in feet
        
def calculateDiff(BTimeString,ATimeString):
    time=BTimeString[0].split(":")
    hours=int(time[0])
    minutes=int(time[1])
    seconds=int(time[2])
    BTotalSeconds=hours*3600+minutes*60+seconds

    time=ATimeString[0].split(":")
    hours=int(time[0])
    minutes=int(time[1])
    seconds=int(time[2])
    ATotalSeconds=hours*3600+minutes*60+seconds

    return [BTotalSeconds,ATotalSeconds,BTotalSeconds-ATotalSeconds]

#stops
with open(dataLocation+'\\stops.txt', newline='') as stopsFile:
    stopsReader = csv.reader(stopsFile, delimiter=',')
    stopsDicKeys=['stop_id','stop_code','stop_name','stop_desc','stop_lat','stop_lon','zone_id','stop_url','location_type','parent_station','stop_timezone','wheelchair_boarding']
    #print(stopsDicKeys)
    isFirstLine=True
    for row in stopsReader:
        if isFirstLine==True:
            isFirstLine=False
            continue
        rowDic={}
        for key in range(len(stopsDicKeys)):
            rowDic[stopsDicKeys[key]]=row[key]
        rowDic["stop_trip_id"]=[]#record of each stop's trip id
        stopsDic[row[0]]=(rowDic)# The key is stop_id
print("Stops done.")

#trips
with open(dataLocation+'\\trips.txt', newline='') as tripsFile:
    tripsReader = csv.reader(tripsFile, delimiter=',')
    
    tripsDicKeys=['route_id','service_id','trip_id','trip_headsign','trip_short_name','direction_id','block_id','shape_id','wheelchair_accessible','bikes_allowed']
    #print(tripsDicKeys)
    isFirstLine=True
    for row in tripsReader:
        if isFirstLine==True:
            isFirstLine=False
            continue
        rowDic={}
        for key in range(len(tripsDicKeys)):
            rowDic[tripsDicKeys[key]]=row[key]
        tripsDic[row[2]]=(rowDic)
print("Trips done.")

#stop_times
with open(dataLocation+'\\stop_times.txt', newline='') as stopTimesFile:
    stopTimesReader = csv.reader(stopTimesFile, delimiter=',')
    stopTimesDicKeys=['trip_id','arrival_time','departure_time','stop_id','stop_sequence','stop_headsign','pickup_type','drop_off_type','shape_dist_traveled']
    #print(stopTimesDicKeys)
    isFirstLine=True
    for row in stopTimesReader:
        if isFirstLine==True:
            isFirstLine=False
            continue
        if row[1]=="":
            continue
        rowDic={}
        for key in range(len(stopTimesDicKeys)):
            rowDic[stopTimesDicKeys[key]]=row[key]
        
        try:
            stopTimesDic[rowDic["trip_id"]]
        except KeyError:
            stopTimesDic[rowDic["trip_id"]] = {}
            stopTimesDic[rowDic["trip_id"]][rowDic["stop_id"]]=rowDic
        else:
            stopTimesDic[rowDic["trip_id"]][rowDic["stop_id"]]=rowDic
print("Stop_times done.")

'''with open(baseLocation + '\\test.json', 'w') as fp:
    json.dump(stopTimesDic, fp)'''

#To add trip information to stopsDic
#Will be some trips which have some stops but those stops don't have arrival time
trip_id_list=stopTimesDic.keys()
for a_trip_id in trip_id_list:
    stopsAgg=stopTimesDic[a_trip_id]
    stop_id_list=stopsAgg.keys()
    for a_stop_id in stop_id_list:
        stopsDic[a_stop_id]["stop_trip_id"].append(a_trip_id)
'''
with open(dataLocation + '\\stops_aggregated.json', 'w') as fp:
    json.dump(stopsDic, fp)
'''
print("Import done.")

client = MongoClient('mongodb://localhost:27017/')
db = client.cota_gtfs_1
db_transfer=db.transfer

stopsDicKeys=stopsDic.keys()
for BStop_id in stopsDicKeys:#Second part of the trip
    
    B=stopsDic[BStop_id]
    BLat=B["stop_lat"]
    BLon=B["stop_lon"]
    print(BStop_id)
    for AStop_id in stopsDicKeys:#First part of the trip
        A=stopsDic[AStop_id]
        ALat=A["stop_lat"]
        ALon=A["stop_lon"]

        distance=Haversine([BLon,BLat],[ALon,ALat]).meters
        if distance>searchRadius:
            continue
        else:#this pair is a potential one. Gap=walkingtime
            realWalkingTime=distance/walkingSpeed

            for Bj in stopsDic[BStop_id]["stop_trip_id"]:#Bj: The trip_id in a stopDic stop record
                BTimeString=stopTimesDic[Bj][BStop_id]["arrival_time"]#Bjt
                BService_id=tripsDic[Bj]["service_id"]
                closestScheduleDiff=99999#trip_id+stop_id=time and location. AKA Ait
                closestATripId=-1
                closestARouteId=-1
                closestATime=-1
                closestBTime=-1

                
                for Ai in stopsDic[AStop_id]["stop_trip_id"]:#Ai: The trip_id in a stopDic stop record
                    AService_id=tripsDic[Ai]["service_id"]
                    if BService_id=="1" or BService_id=="2" or BService_id=="3":
                        if AService_id!=BService_id:
                            continue
                    if tripsDic[Bj]["route_id"]==tripsDic[Ai]["route_id"]:
                        continue;
                    ATimeString=stopTimesDic[Ai][AStop_id]["arrival_time"]#Ait
                    print(ATimeString,BTimeString)
                    scheduleDiff=calculateDiff(BTimeString,ATimeString)#gap
                    if scheduleDiff[2]<realWalkingTime or scheduleDiff[2]<=0:
                        continue;
                    if closestScheduleDiff>scheduleDiff[2]:
                        closestScheduleDiff=scheduleDiff[2]
                        closestATime=scheduleDiff[1]
                        closestBTime=scheduleDiff[0]
                        closestATripId=Ai
                        closestARouteId=tripsDic[Ai]["route_id"]

                if closestATripId==-1:
                    continue
                line={}
                line["a_stop_id"]=AStop_id
                line["a_trip_id"]=closestATripId
                line["a_service_id"]=tripsDic[closestATripId]["service_id"]
                line["a_time"]=closestATime
                
                line["b_stop_id"]=BStop_id
                line["b_trip_id"]=Bj
                line["b_service_id"]=tripsDic[Bj]["service_id"]
                line["b_time"]=closestBTime

                line["diff"]=closestScheduleDiff
                line["w_time"]=round(realWalkingTime,2)
                
                db_transfer.insert(line)
                
                #print(line)

print("Pairs calculation done.")

'''
for key in transferDic.keys():
    with open(dataLocation + '\\transferDic'+key+'.json', 'w') as fp:
        json.dump(transferDic[key], fp)

with open(dataLocation + '\\transferDic.json', 'w') as fp:
    json.dump(transferDic, fp)'''