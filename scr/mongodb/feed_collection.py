from google.transit import gtfs_realtime_pb2
import threading, zipfile, csv, time
import io
import urllib.request as urllib
from pymongo import MongoClient

##collect all stop time sequences      
def CollectingTripupdate():       
    feed = gtfs_realtime_pb2.FeedMessage()
    response = urllib.urlopen('http://realtime.cota.com/TMGTFSRealTimeWebService/TripUpdate/TripUpdates.pb')
    feed.ParseFromString(response.read())
    entities = []
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip_id = entity.trip_update.trip.trip_id
            route_id = entity.trip_update.trip.route_id
            vehicle_id = entity.trip_update.vehicle.id
            vehicle_label = entity.trip_update.vehicle.label
            start_date = entity.trip_update.trip.start_date
            timestamp = entity.trip_update.timestamp
            stop_time_update = entity.trip_update.stop_time_update
            array_stop_time_update = []
            if len(stop_time_update) == 0:continue
            for i in range(len(stop_time_update)):
                stop_update = {"stop_sequence":None, "stop_id":None, "arrival":None}
                stop_update["stop_sequence"] = stop_time_update[i].stop_sequence
                stop_update["stop_id"] = stop_time_update[i].stop_id
                stop_update["arrival"] = stop_time_update[i].arrival.time
                array_stop_time_update.append(stop_update)
            
            entities.append({"trip_id":trip_id, "route_id":route_id, "vehicle_id":vehicle_id, "vehicle_label":vehicle_label, "start_date":start_date, "timestamp":timestamp, "stop_time_update":array_stop_time_update})
    return [entities, len(feed.entity), len(entities)]

def CollectingVehicleposition():       
    feed = gtfs_realtime_pb2.FeedMessage()
    response = urllib.urlopen('http://realtime.cota.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb')
    feed.ParseFromString(response.read())
    entities = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            trip_id = entity.vehicle.trip.trip_id
            route_id = entity.vehicle.trip.route_id
            start_date = entity.vehicle.trip.start_date
            timestamp = entity.vehicle.timestamp
            latitude = entity.vehicle.position.latitude
            longitude = entity.vehicle.position.longitude
            bearing = entity.vehicle.position.bearing
            speed = entity.vehicle.position.speed
            vehicle_id = entity.vehicle.vehicle.id
            vehicle_label = entity.vehicle.vehicle.label
            entities.append({"trip_id":trip_id, "route_id":route_id, "start_date":start_date, "timestamp":timestamp, "latitude":latitude, "longitude":longitude, "bearing":bearing, "speed":speed, "vehicle_id":vehicle_id, "vehicle_label":vehicle_label})
    return [entities, len(feed.entity), len(entities)]

def CollectingGTFSstatic():
    filehandle, _ = urllib.urlretrieve('https://www.cota.com/COTA/media/COTAContent/OpenGTFSData.zip')
    gtfs_zipfile = zipfile.ZipFile(filehandle, 'r')
    gtfs_filelist = gtfs_zipfile.namelist()
    dic_gtfs = {}
    for i in gtfs_filelist:
        print(i)
        reader =gtfs_zipfile.read(i).decode("UTF8").split("\r\n")
        reader.pop()
        header = reader[0].split(',')
        db[i.split(".")[0]].drop()
        db_collection = db[i.split(".")[0]]
        reader.pop(0)
        if i=="fare_attributes":
            print(reader)
        for j in reader:
            if j == '':continue 
            j=j.split(",")
            row = {}
            if len(j)==len(header):
                for k in range(len(header)):
                    row[header[k]] = j[k]
                db[i.split(".")[0]].insert(row)
            else:
                if len(j)==len(header)+1:
                    for k in range(len(j)):
                        if k<len(header):
                            if j[k]=='':
                                continue
                            if j[k][0]==' ':
                                threshold=k-1
                    for k in range(len(header)):
                        if k<threshold:
                            row[header[k]] = j[k]
                        if k==threshold:
                            row[header[k]] = j[k]+j[k+1]
                        if k==threshold+1:
                            continue;
                        if k>threshold+1:
                            row[header[k]] = j[k+1]
                    db[i.split(".")[0]].insert(row)

client = MongoClient('mongodb://localhost:27017/')
db = client.cota_gtfs_1
db_tripupdate = db.tripupdate
db_vehicle = db.vehicle

def RepeatFeedCollection():
    print(time.asctime(time.localtime(time.time())), 'Feed Starting')
    threading.Timer(60.0, RepeatFeedCollection).start()
    try:
        db_tripupdate.insert_many(CollectingTripupdate()[0])
    except:
        print("error in collecting tripupdate " +time.asctime(time.localtime(time.time())))
        pass
    try:
        db_vehicle.insert_many(CollectingVehicleposition()[0])
    except:
        print("error in collecting vehicle position " +time.asctime(time.localtime(time.time())))
        pass
    print(time.asctime(time.localtime(time.time())), 'Feed Exiting')

#RepeatFeedCollection()

def RepeatGTFSstatic():
    print(time.asctime(time.localtime(time.time())), 'GTFS Starting')
    threading.Timer(86400.0, RepeatGTFSstatic).start()
    try:
        CollectingGTFSstatic()
    except:
        print ("error in collecting GTFS static " + time.asctime(time.localtime(time.time())))
        pass
    print (time.asctime(time.localtime(time.time())), 'GTFS Exiting')

CollectingGTFSstatic()