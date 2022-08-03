from cloudreports import database, client
import requests
import json
import datetime


# Data destination initialization
# TODO(developer): Change project, dataset and key_path to the path to the Google BigQuery service
#                  account key file.
#                  See https://cloud.google.com/iam/docs/creating-managing-service-account-keys

project = 'my_project'
dataset = 'my_dataset'
key_path = 'credentials-bigquery.json'

db = database.BigQuery(project, dataset, credentials_file_path=key_path)
client = client.Client(db)

# Load examples data from https://api.nasa.gov
# NeoWs data
r = requests.get('https://api.nasa.gov/neo/rest/v1/feed?start_date=2015-09-08&end_date=2015-09-09&api_key=DEMO_KEY')
r = r.json()
for key, value in r['near_earth_objects'].items():
    date = datetime.datetime.strptime(key, '%Y-%m-%d')
    for row in value:        
        # send data to BigQuery
        # load_json_data method arguments:
        #   entity_id - entity unique identifier, e.g. document number
        #   entity_type - e.g. document type
        #   entity_data - data from API in json format
        #   event_moment - date the entity was modified or created
        client.load_json_data(entity_id=row['id'], entity_type='NeoWs',
            entity_data=row, event_moment=date)

# DONKI data
r = requests.get('https://api.nasa.gov/DONKI/CME?startDate=2017-01-01&endDate=2017-01-31&api_key=DEMO_KEY')
r = r.json()
for row in r:     
    date = datetime.datetime.strptime(row['startTime'], '%Y-%m-%dT%H:%MZ') 
    # send data to BigQuery
    client.load_json_data(row['activityID'], 'DONKI', row, date)

# must be called before completion
client.finish_load_json_data()   

# adding views for new entity types
db.update_tables()
