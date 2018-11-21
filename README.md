# transfer

## Introduction
These separate python files will catch, format, organize, compute, finally analyze the transfer behavior in a public transportation system with the support of GTFS static and real-time.

## Excecute tips
Following two parts is not dependent with each other. Other processes are dependent with the former scripts.

### I. GTFS Real-time
1. You need to use scr/mongodb/feed_collection.py to collect data from the streaming website. Better solution will be a server with python script catching real-time feed and add to a Mongodb database.
2. After getting the huge streaming database, run divide_collection.py to divide the huge collection into several individual collection of each day.
3. Then, run find_real_time.py to find the most accurate time from the real-time data.

After running all of these above, you should be able to get a database of each day's all trips' real departure/arrival time at each stop. Please be advised that this time is independent from the GTFS static (though when the PT system agency is producing and streaming the real-time data, they will try to make them consistent), which means we can do all of these above regardless of the GTFS static data.

### II. GTFS static
1. Run scr/mongodb/feed_export_tripupdate_schedule_separation.py to get GTFS static from an online source (a zip file) and store it to a Mongo database with each file in the zip as a collection.
2. You may want to revise these scripts since pratically I did not write these scripts. I only use these data exported from the data server. I used "collection_name"+ "_"+timestamp to name the collection to avoid ambiguous naming.

### III. find transfer
Since transfer schedule is not explicitly defined in the GTFS schedule, we have to find all possible and closest transfer. Please check my paper to get more information about this.
The generated transfer schedule file will be stored at gtfs database as "transfers" collection. So, please make sure 
