import os
import json
import datetime
from supabase import create_client, Client


def filterFrancoData():
    SUPABASE_URL = os.environ.get(SUPABASE_URL)
    SUPABASE_KEY = os.environ.get(SUPABASE_KEY)

    url: str = SUPABASE_URL
    key: str = SUPABASE_KEY
    supabase: Client = create_client(url, key)

    response = supabase.table('device_raw_airplay').select("*").execute()
    res = supabase.rpc('truncate_franco_data', {})
    res.execute()
    
    data_list = response.data

    #Create an empty list to hold viewed records
    records_viewed = []
    same_title = []
    same_artist = []

    #Iterate over the records and add them only if they are not duplicates
    new_records = []
    duplicate_records = []
    for record in data_list:
        #Check if the record is already in the viewed records list
        duplicate = False
        last_viewed = records_viewed[-50:]
        for viewed in last_viewed:
            if viewed['market'] == record['market'] and viewed['country'] == record['country'] and viewed['station'] == record['station'] and viewed['frequency'] == record['frequency'] and viewed['title'] == record['title'] and viewed['artist'] == record['artist'] and viewed['acr_id'] == record['acr_id']:
                #The record is already in the viewed records list
                duplicate = True
                duplicate_records.append(record)
                break
            if viewed['title'] == record['title'] and viewed['market'] == record['market'] and viewed['country'] == record['country'] and viewed['station'] == record['station'] and viewed['frequency'] == record['frequency']:
                duo = (viewed, record)
                same_title.append(duo)
                timestamp1 = viewed['timestamp']
                timestamp2 = record['timestamp']
                dt1 = datetime.datetime.strptime(timestamp1, "%Y-%m-%dT%H:%M:%S")
                dt2 = datetime.datetime.strptime(timestamp2, "%Y-%m-%dT%H:%M:%S")
                delta = dt2 - dt1
                if delta.total_seconds() < 300:
                    duplicate = True
                    duplicate_records.append(record)
                    break
            if viewed['artist'] == record['artist']:
                duo = (viewed, record)
                same_artist.append(duo)
        if not duplicate:
            #Add the record to the list of viewed records
            records_viewed.append(record)
            new_records.append(record)

    #Insert the new records into the supabase table
    insertResponse = supabase.table(table_name="franco_data").insert(new_records).execute()