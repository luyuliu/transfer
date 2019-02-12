baseLocation = "I:\\OSU\\Transfer"
dataLocation = "I:\\OSU\\data\\data\\OpenGTFSData_201802"
import csv
import json
import math
from pymongo import MongoClient
stopsDic = {}
stopTimesDic = {}
tripsDic = {}
transferDic = {}

walkingSpeed = 1.4  # meters per second
walkingTime = 71.43  # seconds // In order to limit the number of transfer pair
searchRadius = walkingSpeed * walkingTime


class Haversine:
    '''
    use the haversine class to calculate the distance between
    two lon/lat coordnate pairs.
    output distance available in kilometers, meters, miles, and feet.
    example usage: Haversine([lon1,lat1],[lon2,lat2]).feet

    '''

    def __init__(self, coord1, coord2):
        lon1, lat1 = coord1
        lon2, lat2 = coord2
        lon1 = float(lon1)
        lon2 = float(lon2)
        lat1 = float(lat1)
        lat2 = float(lat2)

        R = 6371000                               # radius of Earth in meters
        phi_1 = math.radians(lat1)
        phi_2 = math.radians(lat2)

        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0)**2 +\
            math.cos(phi_1) * math.cos(phi_2) *\
            math.sin(delta_lambda / 2.0)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        self.meters = R * c                         # output distance in meters
        self.km = self.meters / 1000.0              # output distance in kilometers
        self.miles = self.meters * 0.000621371      # output distance in miles
        self.feet = self.miles * 5280               # output distance in feet


def calculateDiff(BTimeString, ATimeString):
    time = BTimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    BTotalSeconds = hours * 3600 + minutes * 60 + seconds

    time = ATimeString.split(":")
    hours = int(time[0])
    minutes = int(time[1])
    seconds = int(time[2])
    ATotalSeconds = hours * 3600 + minutes * 60 + seconds

    return [BTotalSeconds, ATotalSeconds, BTotalSeconds - ATotalSeconds]

def returnRouteID(trip_id):
    target=tripsDic[trip_id]
    tag = 0
    if tripsDic[trip_id]["direction_id"] == "0":
        tag = 1
    elif tripsDic[trip_id]["direction_id"] == "1":
        tag = -1
    else:
        return False
    
    return int(target["route_id"])*tag


client = MongoClient('mongodb://localhost:27017/')
db_GTFS = client.cota_gtfs

db_time_stamps_set=set()
db_time_stamps=[]
raw_stamps=db_GTFS.collection_names()
for each_raw in raw_stamps:
    each_raw=int(each_raw.split("_")[0])
    db_time_stamps_set.add(each_raw)

for each_raw in db_time_stamps_set:
    db_time_stamps.append(each_raw)
db_time_stamps.sort()

for each_time_stamp in db_time_stamps:
    if each_time_stamp < 1546887112:
        continue
    print(each_time_stamp)
    db_seq=db_GTFS[str(each_time_stamp)+"_trip_seq"]
    db_stops=db_GTFS[str(each_time_stamp)+"_stops"]
    db_stop_times=db_GTFS[str(each_time_stamp)+"_stop_times"]
    db_trips=db_GTFS[str(each_time_stamp)+"_trips"]
    stopsList = list(db_stops.find({}))
    for i in range(len(stopsList)):
        stopsList[i]["stop_trip_id"] = []
        stopsDic[stopsList[i]["stop_id"]] = stopsList[i]
    stopsLength=len(stopsList)# length of stopsLIst

    tripsList = list(db_trips.find({}))
    for i in range(len(tripsList)):
        tripsDic[tripsList[i]["trip_id"]] = tripsList[i]

    stopTimesList = list(db_stop_times.find({}))
    for eachStopTimes in stopTimesList:
        stopsDic[eachStopTimes["stop_id"]]["stop_trip_id"].append(eachStopTimes["trip_id"])
        try:
            stopTimesDic[eachStopTimes["trip_id"]]
        except KeyError:
            stopTimesDic[eachStopTimes["trip_id"]] = {}
        else:
            pass
        stopTimesDic[eachStopTimes["trip_id"]][eachStopTimes["stop_id"]]=eachStopTimes
    print("Import done.")
    #print(tripsDic)
    db_transfer = db_GTFS[str(each_time_stamp)+"_transfers"]

    stopsDicKeys = stopsDic.keys()
    stopsCount=0
    transferTotalCount=0
    # A stops iteration begins
    for AStopID in stopsDicKeys:  # First part of the trip
        stopsCount=stopsCount+1
        A = stopsDic[AStopID]
        ALat = A["stop_lat"]
        ALon = A["stop_lon"]

        stopPairsDic = {}  # For each generating stop, it will maintain a dic storing:
        # 1. All the combination of routes; So the tree's key/leaf is route number.
        # route number: if direction_id=0 then tag=1; if direction_id=1 then tag=-1;
        # 2. For each route pair, there are only one series of trip pairs among all the possible stop pairs.
        # For one generating stop, we will select the nearest stop out of all the possible stops for one route pair.
        # The selectin process is separately conducted for each route pair, and we will only include the nearest's stop's trips pairs into the lines dictionary.
        # 3. For one route pair, the leaf contains: 1. line; 2. walking time. Shorter walking time means better.

        ## B stops iteration begins
        for BStopID in stopsDicKeys:  # Second part of the trip
            B = stopsDic[BStopID]
            BLat = B["stop_lat"]
            BLon = B["stop_lon"]

            distance = Haversine([BLon, BLat], [ALon, ALat]).meters
            if distance > searchRadius:
                continue
            else:  # this pair is a potential one. Gap=walkingtime
                realWalkingTime = round(distance / walkingSpeed,2)

                # A trips iteration begins
                # ATripID: The trip_id in a stopDic stop record


                #Todo: line should be maintained just the same as the lines.
                microDic={}# Single stop pair's trip pairs dictionary.
                for ATripID in stopsDic[AStopID]["stop_trip_id"]:
                    AServiceID = tripsDic[ATripID]["service_id"]
                    ARouteID=returnRouteID(ATripID)
                    ATimeString = stopTimesDic[ATripID][AStopID]["arrival_time"]  # Ait
                    
                    #Service control:
                    if AServiceID != "1" and AServiceID != "2" and AServiceID != "3":
                        continue;

                    # B trips iteration begins
                    # BTripID: The trip_id in a stopDic stop record
                    for BTripID in stopsDic[BStopID]["stop_trip_id"]:
                        BServiceID = tripsDic[BTripID]["service_id"]
                        BRouteID=returnRouteID(BTripID)

                        # Service ID control: right now just look at 1 2 3. Let aside all others.
                        if BServiceID != "1" and BServiceID != "2" and BServiceID != "3":
                            continue;
                        else:
                            if AServiceID != BServiceID:
                                continue

                        # Service ID control ends.

                        ## The exceptions.
                        # Same route doesn't count as a transfer, regardless of the direction
                        if tripsDic[BTripID]["route_id"] == tripsDic[ATripID]["route_id"]:# this route_id is abs.
                            continue
                        # The arrival/departure time at the B stop
                        BTimeString = stopTimesDic[BTripID][BStopID]["arrival_time"]
                        scheduleDiff = calculateDiff(
                            BTimeString,  ATimeString)  # gap
                        # The condition for non-transfers
                        if scheduleDiff[2] < realWalkingTime or scheduleDiff[2] <= 0 or scheduleDiff[2] >= 1800:
                            continue
                        ## The exception ends.

                        # For a distint route-pair, there will be multiple trip pairs. And a_trip_id can be the primary index for them, since for a transfer
                        # from Route A to Route B, one trip of route A can only have one corresponding best (scheduled) trip of route B.
                        try:
                            microDic[ARouteID]
                        except KeyError:
                            microDic[ARouteID] = {}
                        else:
                            pass

                        try:
                            microDic[ARouteID][BRouteID]
                        except KeyError:
                            microDic[ARouteID][BRouteID] = {}
                        else:
                            pass
                        
                        try:
                            microDic[ARouteID][BRouteID][ATripID]
                        except KeyError: # The record doesn't exist. So it must be the first trip pairs we encounter. So just write it to the microDic.
                            # trip pairs build-up starts.
                            # initialization. All of them.
                            line = {}

                            line["a_stop_id"] = AStopID
                            line["a_trip_id"] = ATripID
                            line["a_service_id"] = AServiceID
                            line["a_time"] = scheduleDiff[1]
                            line["a_route_id"] = ARouteID

                            line["b_stop_id"] = BStopID
                            line["b_trip_id"] = BTripID #to be updated
                            line["b_service_id"] = BServiceID #to be updated
                            line["b_time"] = scheduleDiff[0] #to be updated
                            line["b_route_id"] = BRouteID

                            line["diff"] = scheduleDiff[2] #to be updated
                            line["w_time"] = realWalkingTime # will always be the same so don't have to change.
                            # trip pairs build-up ends.
                            microDic[ARouteID][BRouteID][ATripID] = line
                            # Ends. To next BTripID
                        else: # The record exist, so compare them to update the microDic
                            if microDic[ARouteID][BRouteID][ATripID]["diff"]>scheduleDiff[2]:
                                line["b_trip_id"] = BTripID #to be updated
                                line["b_service_id"] = BServiceID #to be updated
                                line["b_time"] = scheduleDiff[0] #to be updated
                                line["diff"] = scheduleDiff[2] #to be updated

                    # B trips iteration ends

                # A trip iteration ends. Current stop pair ends.
                
                ## Incorporation starts.
                # Incorporate the microDic to the stopPairDic
                # The idea is basically search the current microDic and put each leaf to the stopPairDic
                
                ARouteIDKeys=microDic.keys()
                
                for iARouteID in ARouteIDKeys:
                    BRouteIDKeys=microDic[iARouteID].keys()
                    
                    for jBRouteID in BRouteIDKeys:
                        try:
                            stopPairsDic[iARouteID]
                        except KeyError:
                            stopPairsDic[iARouteID] = {}
                        else:
                            pass

                        try:
                            stopPairsDic[iARouteID][jBRouteID]
                        except KeyError:
                            stopPairsDic[iARouteID][jBRouteID] = {"tripPairsList":microDic[iARouteID][jBRouteID],
                                "w_time": realWalkingTime}
                        else:
                            if stopPairsDic[iARouteID][jBRouteID]["w_time"] > realWalkingTime:
                                stopPairsDic[iARouteID][jBRouteID] = {"tripPairsList":microDic[iARouteID][jBRouteID],
                                "w_time": realWalkingTime}
                    
                ## Incorporation ends.
        ## B stops iteration ends


        transferCount=0
        # Database push starts
        if stopPairsDic=={}:
            print("-----------------"+str(AStopID)+ " : ("+str(stopsCount)+" / "+str(stopsLength)+") : [ "+str(transferCount)+" | "+str(transferTotalCount)+" ] -----------------")
            continue

        ARouteIDKeys=stopPairsDic.keys()
        for iARouteID in ARouteIDKeys:
            BRouteIDKeys=stopPairsDic[iARouteID].keys()
            for jBRouteID in BRouteIDKeys:
                transferIDKeys=stopPairsDic[iARouteID][jBRouteID]["tripPairsList"].keys()
                for aTransfer in transferIDKeys:
                    db_transfer.insert(stopPairsDic[iARouteID][jBRouteID]["tripPairsList"][aTransfer])
                    transferCount=transferCount+1
        # Database push ends
        
        transferTotalCount=transferTotalCount+transferCount
        print("-----------------"+str(AStopID)+ " : ("+str(stopsCount)+" / "+str(stopsLength)+") : [ "+str(transferCount)+" | "+str(transferTotalCount)+" ] -----------------")





    # A stops iteration ends

    print("Pairs calculation done.")
