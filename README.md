# transfer

## Introduction
These separate python files will catch, format, organize, compute, finally analyze the transfer behavior in a public transportation system with the support of GTFS static and real-time.

## Excecute tips
GTFS Real-time and GTFS static is not dependent with each other. Other processes are dependent with the former scripts.

### I. GTFS Real-time
1. You need to use scr/mongodb/feed_collection.py to collect data from the streaming website. Better solution will be a server with python script catching real-time feed and add to a Mongodb database.
2. After getting the huge streaming database, run divide_collection.py to divide the huge collection into several individual collection of each day.
3. Then, run find_real_time.py to find the most accurate time from the real-time data.

After running all of these above, you should be able to get a database of each day's all trips' real departure/arrival time at each stop. Please be advised that this time is independent from the GTFS static (though when the PT system agency is producing and streaming the real-time data, they will try to make them consistent), which means we can do all of these above regardless of the GTFS static data.

### II. GTFS Static
1. Run scr/mongodb/feed_export_tripupdate_schedule_separation.py to get GTFS static from an online source (a zip file) and store it to a Mongo database with each file in the zip as a collection.
2. You may want to revise these scripts since pratically I did not write these scripts. I only use these data exported from the data server. I used "collection_name"+ "_"+timestamp to name the collection to avoid ambiguous naming.

### III. Additional GTFS Static
After finishing II:
1. Run scr/transfer/find_transfer.py to generate the transfer schedule. Since transfer schedule is not explicitly defined in the GTFS schedule, we have to find all possible and closest transfer. Please check my paper to get more information about this. The generated transfer schedule file will be stored at gtfs database as "transfers" collection. So, please make sure to check if your GTFS static has an vanilla version of transfers file.
2. Run scr/transfer/sort_stop_time.py to generate the trip_seq collection, which represents the sequence of each route's trips. 
For example, route 1 will go through stop "RIVHOSW". And there will be several different buses with different trip_id. Then trip_seq collection is to record these trips' temporal sequence at this stop. The sequence will start from 0.

### IV. Validate Transfer
After finishing I, II, and III, run scr/transfer/validate_transfer.py to validate the transfers. The script will find the scheduled transfers' status, including actual receiving trip， status, tr Please read my paper to know more details.
The script will produce a database with each day's all scheduled transfer as collection.

#### Database key explanation:
| Key        | Meaning           | Type  |   Detailed explanation   |
| ------------- |:-------------:| :-----:|  -------------------------: |
| id      | mongodb id | hexString | 
| a_st      | a_stop_id      |   String | The id of generating stop's id |
| a_tr |  a_trip_id      |    String | The id of generating trip's id |
| a_ro |  a_route_id      |    Int32 | The id of generating route's id |
| a_ro |  a_t      |    Int32 |  The scheduled time of generating trip's arrival time on generating stop |
| b_st      | b_stop_id      |   String | The id of receiving stop's id |
| b_tr |  b_trip_id      |    String | The id of receiving trip's id |
| b_ro |  b_route_id      |    Int32 | The id of receiving route's id |
| b_ro |  b_t      |    Int32 |  The scheduled time of receiving trip's departure time on receiving stop |
| w_t |  walking_time      |    Double | The theoretical walking time from generating stop to receiving stop |
| schd_diff |  scheduled_difference      |    Int32 | The scheduled waiting time/buffer time |
| b_r_t |  b_real_time      |    Int32 | The actual departure time of scheduled receiving bus |
| a_r_t |  a_real_time      |    Int32 | The actual arrival time of scheduled generating bus|
| diff |  difference      |    Int32 | The actual waiting time/buffer time |
| status |  status      |    Int32 | The status of this transfer |
| b_a_t |  b_alternative_time      |    Int32 | The actual departure time of actual receiving bus |
| b_a_seq |  b_alternative_seq     |    Int32 | The sequence of actual receiving bus in the trip_seq collection |
| b_a_tr |  b_alternative_trip_id      |    Int32 | The trip_id of actual receiving bus |

#### Status explanation:
| Status        | Meaning           |
| ------------- |:-------------:|
| 0      | normal transfer (N=0) | 
| 1      | missed transfer (N>0) | 
| 2      | preemptive transfer (N<0) | 
| 3      | No stop detected in the feed for the generating trip (missing_a) | 
| 4      | No stop detected in the feed for the receiving trip (missing_b) | 
| 5      | missing_real_time records | 
| 6      | critical transfers (no possible receiving trip found) | 

### V. Analyze Transfer
After finishing IV, run scr/transfer/analysis/analyze_transfer.py to analyze the transfers. The script will export a shapefile, including each stop's average time penalty, transfer risk, and transfers of different status's count.

### VI. Dedicated Bus Lane Simulation
After finishing IV, run scr/transfer/dedicated_transfer.py to simulate dedicated bus lane's impact. 

