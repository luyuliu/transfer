for each_trip in false_trips_list:  # For each trips, find the real departure time for it, which still satisfies the requirement: the departure time is after
                                    # the user's arrival time (stop A arrival time + walking time).
                                    # Find the relative real time of departure from tripupdate database is not very easy since we have to look up every records
                                    # in the query results, and find the nearest stop. But since in the query result, the sequence is already from the start to destination.
                                    # So just go to the last record, and get the b_alt_time of this record.
                                    # Then find the smallest/closest trip as the alternative trip for the trip B.
    
    

    

    each_trip["sequence_id"] = i_sequence_id
    
    if i_trip_id == b_trip_id:
        flag_sequence_id = i_sequence_id

    i_sequence_id = i_sequence_id + 1
end_time = time.time()
print(end_time - start_time)

for each_trip in false_trips_list:
    i_trip_id = each_trip["trip_id"]
    stop_time_query = list(db_GTFS.stop_times.find(
        {"trip_id": i_trip_id, "stop_id": b_stop_id}))
    if len(stop_time_query) == 0:
        each_trip["sequence_id"] = None
        continue
    each_trip["sequence_id"] = i_sequence_id
    each_trip["b_time"] = stop_time_query[0]["arrival_time"]
    if i_trip_id == b_trip_id:
        flag_sequence_id = i_sequence_id
    each_b_time = 0
    
    if each_trip["sequence_id"] == None:
        continue
    # Find the b_alt_time for this trip_id and compare it to the b_alt_time (overall). Until we find the smallest one.
    # The most accurate feed must be the last one of the query result. (To be proven)
    start_time = time.time()
    alternative_b_trip = list(db_tripupdate.find({"trip_id": str(i_trip_id),
                                                    "seq": {"$elemMatch": {"stop": b_stop_id}}}))
    end_time = time.time()
    print("!", end_time - start_time)
    '''print("length:",len(alternative_b_trip))
    for each_feed in alternative_b_trip:
        tag=0
        for each_stop_time in each_feed["seq"]:
            if each_stop_time["stop"] == b_stop_id:
                print(each_stop_time["arr"],tag)
                break
            tag=tag+1'''
    if len(alternative_b_trip) == 0:
        # print("i_trip_id",i_trip_id,"|","each_schedule",convertSeconds(each_trip["b_time"],single_date),"each_b_time",each_b_time,"id","|",each_trip["sequence_id"])
        continue
    for b_stop_time in alternative_b_trip[len(alternative_b_trip) - 1]["seq"]:
        if b_stop_time["stop"] == b_stop_id:
            each_b_time = b_stop_time["arr"]
        if b_stop_time["stop"] == b_stop_id and b_alt_time > b_stop_time["arr"] and b_stop_time["arr"] >= a_real_time + real_transfer["w_t"]:
            b_alt_time = b_stop_time["arr"]
            b_alt_sequence_id = each_trip["sequence_id"] - \
                flag_sequence_id
            b_alt_trip_id = i_trip_id
            break
    # print("i_trip_id",i_trip_id,"|","each_schedule",convertSeconds(each_trip["b_time"],single_date),"each_b_time",each_b_time,"id","|",each_trip["sequence_id"])
    i_sequence_id = i_sequence_id + 1
    
# This means there's no alternative trip for this receiving trip. So you are doomed.
if b_alt_time == 9999999999:
    b_alt_time = -1  # there's no an alternative trip.
    b_alt_sequence_id = None
    b_alt_trip_id = "-1"
