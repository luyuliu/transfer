import shapefile
from pymongo import MongoClient
from datetime import timedelta, date
client = MongoClient('mongodb://localhost:27017/')

db_GTFS = client.cota_gtfs_1
db_transfer = db_GTFS.transfer

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def switch_status(status,line):
    if status==0:
        line['zero_count']+=1
    elif status==1:
        line['one_count']+=1
    elif status==2:
        line['two_count']+=1
    else:
        line['miss_count']+=1
    
    line['total_count']+=1


'''start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)'''

start_date = date(2018, 1, 29)
end_date = date(2018, 2, 25)

db_history = client.cota_transfer

# main loop
# enumerate every day in the range
for single_date in daterange(start_date, end_date):
    today_date = single_date.strftime("%Y%m%d")  # date
    today_weekday = single_date.weekday()  # day of week
    if today_weekday < 5:
        service_id = 1
    elif today_weekday == 5:
        service_id = 2
    else:
        service_id = 3

    db_today_collection = db_history[today_date]

    db_result=list(db_today_collection.find({}))

    dic_stops={}

    #print(db_result)
    for single_result in db_result:
        a_stop_id=single_result['a_stop_id']
        b_stop_id=single_result['b_stop_id']
        try:
            dic_stops[a_stop_id]
        except:
            line={}
            a_stop=db_GTFS.stops.find({"stop_id":a_stop_id})[0]
            line['lat']=a_stop['stop_lat']
            line['lon']=a_stop['stop_lon']
            line['total_count'] = 0
            line['zero_count'] = 0
            line['one_count'] = 0
            line['two_count'] = 0
            line['miss_count'] = 0
            dic_stops[a_stop_id]=line
        
        switch_status(single_result['status'],dic_stops[a_stop_id])

    location='I:/OSU/Transfer_data/nor_shp/'+today_date
    print(location)
    w = shapefile.Writer(shapeType=1)
    w.field("stop_id","C")
    w.field("total_count","N")
    w.field("zero_count","N")
    w.field("one_count","N")
    w.field("two_count","N")
    w.field("miss_count","N")
    for key, value in dic_stops.items():
        w.record(key,value['total_count'],value['zero_count'],value['one_count'],value['two_count'],value['miss_count'])
        w.point(float(value['lon']),float(value['lat']))

    w.save(location)
        
        

    