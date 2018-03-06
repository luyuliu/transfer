## for mobility map export trip update feeds (their common portion with GTFS static schedules) and GTFS schedules separately!

import time, math, json, threading
from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import OrderedDict

folder_save = "I:\\OSU\\Transfer\\data"

##initial feed variables
dic_tripupdate = OrderedDict()
dic_tripschedule = OrderedDict()

##GTFS static: schedules
dic_service = {}
dic_stops = {}
dic_routeinfo = {}
dic_tripid_to_shapeid = {}
dic_tripid_by_serviceid = {}
dic_tripinfo = {}
dic_route_by_shapeid = {}
dic_stop_times = {}
dic_stop_times_aggregate = {}
dic_stop_times_arcs = {}
dic_arcs = {}
max_hour = 8

geojson_stops = {'type':'FeatureCollection', 'features':[]}
geojson_stopnetwork = {'type':'FeatureCollection', 'features':[]}
geojson_roadnetwork = {'type':'FeatureCollection', 'features':[]}

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
        
client = MongoClient('mongodb://localhost:27017/')
db = client.cota_gtfs_1



##call gtfs schedule data
def InitializingGTFS():
    db_calendar = db.calendar.find()
    for i in db_calendar:
        date_start = datetime.strptime(i['start_date'], "%Y%m%d")
        date_end = datetime.strptime(i['end_date'], "%Y%m%d")
        date_days = (date_end - date_start).days + 1 ##including start and end dates
        day_service = [i['monday'],i['tuesday'],i['wednesday'],i['thursday'],i['friday'],i['saturday'],i['sunday']]
        for j in range(date_days):
            day_date = date_start + timedelta(days=j)
            day_seq = day_date.weekday()
            if day_service[day_seq] == '0':continue
            try:
                dic_service[date_start + timedelta(days=j)].append(i['service_id'])
            except KeyError:
                dic_service[date_start + timedelta(days=j)] = [i['service_id']]
    
    db_calendar_dates = db.calendar_dates.find()
    for i in db_calendar_dates:
        date_theday = datetime.strptime(i['date'], "%Y%m%d")
        if i['exception_type'] == '1':
            dic_service[date_theday].append(i['service_id'])
        elif i['exception_type'] == '2':
            dic_service[date_theday].remove(i['service_id'])
        else:
            pass
              
    db_stops = db.stops.find()
    #print(list(db_stops))
    for i in db_stops:
        dic_stops[i['stop_id']] = {}
        dic_stops[i['stop_id']]['stop_name'] = i['stop_name']
        dic_stops[i['stop_id']]['coords'] = [float(i['stop_lon']), float(i['stop_lat'])]
        
    
    db_routes = db.routes.find()
    #print(list(db_routes))
    for i in db_routes:
        dic_routeinfo[i['route_id']] = {}
        try:
            dic_routeinfo[i['route_id']]['route_name'] = i['route_long_name']
        except:
            dic_routeinfo[i['route_id']]['route_name']=''
        dic_routeinfo[i['route_id']]['route_color'] = i['route_color']
        
    db_trips = db.trips.find()
    for i in db_trips:
        try:
            dic_tripinfo[i['shape_id']]
        except KeyError:
            dic_tripinfo[i['shape_id']] = {'route_id':i['route_id'], 'trip_ids': [i['trip_id']]}
        else:
            dic_tripinfo[i['shape_id']]['trip_ids'].append(i['trip_id'])
        
        dic_tripid_to_shapeid[i['trip_id']] = i['shape_id'] ## do not consider multiple trip ids to a common shapeid
        
        try:
            dic_tripid_by_serviceid[i['service_id']]
        except KeyError:
            dic_tripid_by_serviceid[i['service_id']] = [i['trip_id']]
        else:
            dic_tripid_by_serviceid[i['service_id']].append(i['trip_id'])
        
    db_shapes = db.shapes.find()
    for i in db_shapes:
        try:
            dic_route_by_shapeid[i['shape_id']]
        except KeyError:
            dic_route_by_shapeid[i['shape_id']] = [[float(i['shape_pt_lon']), float(i['shape_pt_lat'])]]
        else:
            dic_route_by_shapeid[i['shape_id']].append([float(i['shape_pt_lon']), float(i['shape_pt_lat'])])
                        
    db_stop_times = db.stop_times.find()
    for i in db_stop_times:
        trip_id = i['trip_id']
        stop_seq = int(i['stop_sequence'])
        stop_id = i['stop_id']
        try:
            dic_stop_times[trip_id]
        except KeyError:
            dic_stop_times[trip_id] = {}
            dic_stop_times[trip_id][stop_seq] = {}
            dic_stop_times[trip_id][stop_seq][stop_id] = i['arrival_time']
        else:   
            try:
                dic_stop_times[trip_id][stop_seq]
            except KeyError:
                dic_stop_times[trip_id][stop_seq] = {}
                dic_stop_times[trip_id][stop_seq][stop_id] = i['arrival_time']
        
        try:
            dic_stop_times_arcs[trip_id]
        except KeyError:
            dic_stop_times_arcs[trip_id] = {'seq_stop':[i['stop_id']], 'seq_distance': [0]}
        else:
            if type(dic_stop_times_arcs[trip_id]) == dict:
                dic_stop_times_arcs[trip_id]['seq_stop'].append(stop_id)
                dic_stop_times_arcs[trip_id]['seq_distance'].append(float(i['shape_dist_traveled']))
    
    for i in dic_stop_times:
        dic_stop_times_aggregate[i] = []
        seqs = sorted(dic_stop_times[i].keys())
        for j in seqs:
            stop = dic_stop_times[i][j].keys()[0]
            arr = [int(x) for x in dic_stop_times[i][j][stop].split(':')]
            dic_stop_times_aggregate[i].append({'stop_id':stop, 'stop_sequence':j, 'scheduled_arrival':arr})
    
    for i in dic_stop_times_arcs:
        arcs = zip(dic_stop_times_arcs[i]["seq_stop"], dic_stop_times_arcs[i]["seq_stop"][1:])
        distances = zip(dic_stop_times_arcs[i]["seq_distance"], dic_stop_times_arcs[i]["seq_distance"][1:])
        dic_stop_times_arcs[i]["seq_stop"] = arcs
        dic_stop_times_arcs[i]["seq_distance"] = [y-x for x, y in distances]
    
    for i in dic_stop_times_arcs:
        arcs = dic_stop_times_arcs[i]["seq_stop"]
        distances = dic_stop_times_arcs[i]["seq_distance"]
        if len(arcs) != len(distances):
            print (i + " is error")
        
        for j in range(len(arcs)):
            try:
                dic_arcs[arcs[j]]
            except KeyError:
                dic_arcs[arcs[j]] = {"tripid": i, "distance": distances[j]}
            else:
                continue
    
    for i in dic_arcs:
        stop_o = i[0]
        stop_d = i[1]
        stop_o_coords = dic_stops[stop_o]["coords"]
        stop_d_coords = dic_stops[stop_d]["coords"]
        tripid = dic_arcs[i]["tripid"]
        shapeid = dic_tripid_to_shapeid[tripid]
        bustrip_coords = dic_route_by_shapeid[shapeid]
        
        dists_o = []
        dists_d = []
        for j in range(len(bustrip_coords)):
            dists_o.append(Haversine(stop_o_coords, bustrip_coords[j]).miles)
            dists_d.append(Haversine(stop_d_coords, bustrip_coords[j]).miles)
        index_o = dists_o.index(min(dists_o))
        index_d = dists_d.index(min(dists_d))
        
        if index_o < index_d:
            dic_arcs[i]["coords"] = [stop_o_coords] + bustrip_coords[index_o+1:index_d] + [stop_d_coords]
        else:
            dic_arcs[i]["coords"] = [stop_d_coords] + bustrip_coords[index_d+1:index_o] + [stop_o_coords]              
                                    
    ##geojson export
    for i in dic_stops:
        stop = {}
        stop['type'] = "Feature"
        stop['geometry'] = {}
        stop['geometry']['type'] = "Point"
        stop['geometry']['coordinates'] = dic_stops[i]['coords']
        stop['properties'] = {}
        stop['properties']['stop_id'] = i
        stop['properties']['stop_name'] = dic_stops[i]['stop_name']
        geojson_stops['features'].append(stop)
    
    for i in dic_route_by_shapeid:
        route = {}
        route['type'] = 'Feature'
        route['geometry'] = {}
        route['geometry']['type'] = 'LineString'
        route['geometry']['coordinates'] = dic_route_by_shapeid[i]
        route['properties'] = {}
        route['properties']['shape_id'] = i
        route['properties']['route_id'] = dic_tripinfo[i]['route_id']
        route['properties']['trip_ids'] = dic_tripinfo[i]['trip_ids']
        route['properties']['route_name'] = dic_routeinfo[dic_tripinfo[i]['route_id']]['route_name']
        route['properties']['route_color'] = dic_routeinfo[dic_tripinfo[i]['route_id']]['route_color']
        geojson_roadnetwork['features'].append(route) 
    
    for i in dic_arcs:
        arc = {}
        arc['type'] = 'Feature'
        arc['geometry'] = {}
        arc['geometry']['type'] = 'LineString'
        arc['geometry']['coordinates'] = dic_arcs[i]['coords']
        arc['properties'] = {}
        arc['properties']['stop_o'] = i[0]
        arc['properties']['stop_d'] = i[1]
        arc['properties']['distance'] = dic_arcs[i]['distance']
        geojson_stopnetwork['features'].append(arc)
    with open(folder_save + 'gtfs_stops.geojson', 'w') as fp:
        json.dump(geojson_stops, fp)    
    with open(folder_save + 'gtfs_routes.geojson', 'w') as fp:
        json.dump(geojson_roadnetwork, fp)    
    with open(folder_save + 'gtfs_stops_network_roads.geojson', 'w') as fp:
        json.dump(geojson_stopnetwork, fp)

##realtime feeds
realtime_tripid = []
gtfs_tripid = []
def ConvertingFeeds(timegroup, feeds):
    trip_update = feeds
    
    ###trip update with delay
    dic_joinedfeed = {'timegroup':timegroup, 'features':[]}  
    for j in range(len(trip_update)):
        rec = trip_update[j]
        if rec['stop_time_update'] == [] : continue ##bus trip has been completed but still sending a feed    
        
        trip_id = rec['trip_id']
        
        t_timestamp = rec['timestamp']
        stop_time_update = rec['stop_time_update']
        array_stop_time = []

        out_of_schedule = 0
        for k in range(len(stop_time_update)):
            stop_seq = stop_time_update[k]['stop_sequence']
            stop_id = stop_time_update[k]['stop_id']
            realtime_arr = stop_time_update[k]['arrival']
            
            try:
                scheduled_time = [int(x) for x in dic_stop_times[trip_id][stop_seq][stop_id].split(':')]
            except KeyError: ##temporary or abrupt operations out of the given schedules
                out_of_schedule = 1
                #print trip_id, stop_seq, stop_id
                break
            
            scheduled_date = datetime.strptime(trip_update[j]['start_date'], "%Y%m%d")
            if scheduled_time[0]>=24: ##arrival time in schedule contains hour value >= 24
                scheduled_time[0] -= 24
                scheduled_date += timedelta(days=1)
            scheduled_arr = time.mktime((scheduled_date + timedelta(hours=scheduled_time[0], minutes=scheduled_time[1], seconds=scheduled_time[2])).timetuple())
            diff_to_schedule = scheduled_arr - t_timestamp
            diff_to_realtime = max(diff_to_schedule, realtime_arr - t_timestamp)
            
            if diff_to_schedule <=0:continue
            
            selected_stop = stop_time_update[k]
            selected_stop['scheduled_arrival'] = scheduled_arr
            selected_stop['sec_schedule'] = diff_to_schedule
            selected_stop['sec_realtime'] = diff_to_realtime
            
            array_stop_time.append(selected_stop)
            
        if out_of_schedule == 1:continue
        if len(array_stop_time) == 0:continue
        
        realtime_tripid.append(trip_id) #check

        rec['stop_time_update'] = array_stop_time
        dic_joinedfeed['features'].append(rec)

    return dic_joinedfeed

def ConvertingSchedules(timegroup, operationdate):
    scheduled_date = datetime.strptime(operationdate, "%Y%m%d")
    serviceids_theday = dic_service[scheduled_date]
    tripids_theday = []
    dic_stopschedule = {'timegroup':timegroup, 'features':[]}
    for i in serviceids_theday:
        tripids_theday += dic_tripid_by_serviceid[i]
    array_trips = []
    for i in tripids_theday:
        
        s_stop_time = dic_stop_times_aggregate[i][0]['scheduled_arrival']
        e_stop_time = dic_stop_times_aggregate[i][-1]['scheduled_arrival']
        
        s_scheduled_date = scheduled_date
        if s_stop_time[0]>=24:
            s_stop_time[0]-=24
            s_scheduled_date += timedelta(days=1)
        
        e_scheduled_date = scheduled_date    
        if e_stop_time[0]>=24:
            e_stop_time[0]-=24
            e_scheduled_date += timedelta(days=1)
            
        s_timestamp = time.mktime((s_scheduled_date + timedelta(hours=s_stop_time[0], minutes=s_stop_time[1], seconds=s_stop_time[2])).timetuple())
        e_timestamp = time.mktime((e_scheduled_date + timedelta(hours=e_stop_time[0], minutes=e_stop_time[1], seconds=e_stop_time[2])).timetuple())
        
        if (timegroup >= s_timestamp and timegroup < e_timestamp) or (s_timestamp > timegroup and s_timestamp <= timegroup + 2*60*60):
            array_trips.append(i)
    
    array_trips += realtime_tripid
    array_trips = list(set(array_trips))
    print (len(array_trips))
    for i in array_trips:
        gtfs_tripid.append(i) #check
        rec = {'trip_id':i, 'stop_time_update':[]}
        trip_effective = dic_stop_times_aggregate[i]
        array_stop_time = []
        for j in range(len(trip_effective)):
            stop_date = scheduled_date
            stop_time = trip_effective[j]['scheduled_arrival']
            if stop_time[0] >= 24:
                stop_time[0] -= 24
                stop_date += timedelta(days=1)
            stop_timestamp = time.mktime((stop_date + timedelta(hours=stop_time[0], minutes=stop_time[1], seconds=stop_time[2])).timetuple())
            if stop_timestamp - timegroup <=0:continue
            item_stop_time = trip_effective[j]
            item_stop_time['scheduled_arrival'] = stop_timestamp
            rec['stop_time_update'].append(item_stop_time)
    
        dic_stopschedule['features'].append(rec)
    
    return dic_stopschedule
      
def InitializingFeeds():
    print (time.asctime(time.localtime(time.time())), 'Feed initialization Starting')
    dic_timegroup = {}
    time_current = int(math.floor(time.time()/60)*60-60*5) ##check time
    
    db_tripupdate = db.tripupdate
    for i in [0]: #range(max_hour*60):
        dic_timegroup[int(time_current-60*i)] = []

    cursor_tripupdate = db_tripupdate.find({'timestamp': {'$gte': time_current, '$lt': time_current+60}}, {'_id':False}) ##{'$gte': time_current-60*60*max_hour+60, '$lt': time_current+60}})
    for i in cursor_tripupdate:
        dic_timegroup[int(math.floor(i['timestamp']/60)*60)].append(i)
    
    for i in sorted(dic_timegroup.keys()): #dic_timegroup:
        if dic_timegroup[i] == []: 
            continue ##when feeds were not collected at the selected time point 
        dic_tripupdate[i] = ConvertingFeeds(i, dic_timegroup[i])
        
        operationdate = dic_timegroup[i][0]['start_date']
        dic_tripschedule[i] = ConvertingSchedules(i,operationdate)
             
    with open(folder_save + 'cotafeed_joinedfeeds.json', 'w') as fp:
        json.dump(dic_tripupdate, fp)
        
    with open(folder_save + 'cotafeed_gtfsschedule.json', 'w') as fp:
        json.dump(dic_tripschedule, fp)    
    
    print (time.asctime(time.localtime(time.time())), 'Feed initialization Exiting')
    
    ##RepeatUpdatingFeeds()
InitializingGTFS()
InitializingFeeds()



#def UpdatingFeeds():
    
    #time_current = int(math.floor(time.time()/60)*60-60*2)
    
    #timestamp_lowerbound = time_current-60*60*max_hour+60
    #timegroups = sorted(dic_tripupdate.keys())
    #for i in timegroups:
        #if i < timestamp_lowerbound:
            #del dic_tripupdate[i]
            #del dic_vehicle[i]
    
    #timegroups_update = [timegroups[-1] + 60*(x+1) for x in range((time_current-timegroups[-1])/60)]
    
    #db_tripupdate = db.tripupdate
    #db_vehicle = db.vehicle
    
    #for i in timegroups_update:
        #feeds = {"trip_update":[], "vehicle_position":[]}
        
        #cursor_tripupdate = db_tripupdate.find({'timestamp': {'$gte': i, '$lt': i+60}})
        #for j in cursor_tripupdate:
            #feeds['trip_update'].append(j)
        
        #cursor_vehicle = db_vehicle.find({'timestamp': {'$gte': i, '$lt': i+60}})
        #for j in cursor_vehicle:
            #feeds['vehicle_position'].append(j)    
        
        #dic_tripupdate[time_current], dic_vehicle[time_current] = ConvertingFeedsToGeojson(i, feeds)
    
    #print timegroups[-1], timegroups_update, time_current, [len(dic_tripupdate), len(dic_vehicle)]
    
    #with open(folder_save + 'cotafeed_tripupdate.geojson', 'w') as fp:
        #json.dump(dic_tripupdate, fp)    
    #with open(folder_save + 'cotafeed_vehicleposition.geojson', 'w') as fp:
        #json.dump(dic_vehicle, fp)

#def RepeatInitializingGTFS():
    #print time.asctime(time.localtime(time.time())), 'GTFS initialization Starting'
    #threading.Timer(86400.0, RepeatInitializingGTFS).start()
    #try:
        #InitializingGTFS()
    #except:
        #print "error in initializing GTFS static " + time.asctime(time.localtime(time.time()))
        #pass
    #print time.asctime(time.localtime(time.time())), 'GTFS initialization Exiting'    

#def RepeatUpdatingFeeds():
    #print time.asctime(time.localtime(time.time())), 'Feed update Starting'
    #threading.Timer(300.0, RepeatUpdatingFeeds).start()
    #try:
        #UpdatingFeeds()
    #except:
        #print "error in feed update " +time.asctime(time.localtime(time.time()))
        #pass
    #print time.asctime(time.localtime(time.time())), 'Feed update Exiting'

#RepeatInitializingGTFS()  
#InitializingFeeds()
