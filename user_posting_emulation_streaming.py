import sql_engine_connector
import functions

import requests
from time import sleep
import random
import json
import yaml
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

random.seed(100)

def put_api(topic, input_dict):
    invoke_url = f"https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/streams/{topic}/record"
    payload = json.dumps({
        "StreamName": topic,
        "Data": input_dict,
        "PartitionKey": "partition-1"
        })
    headers = {'Content-Type': 'application/json'}

    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response.status_code)
    print(response.json())

def run():
     while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        pin_result = functions.extract_table(new_connector.engine, f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        geo_result = functions.extract_table(new_connector.engine, f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        user_result = functions.extract_table(new_connector.engine, f"SELECT * FROM user_data LIMIT {random_row}, 1")

        pin_dictionary = functions.map_table_to_dictionary(pin_result)
        geo_dictionary = functions.map_table_to_dictionary(geo_result)
        user_dictionary = functions.map_table_to_dictionary(user_result)

        pin_data = functions.correct_date_format(pin_dictionary)
        geo_data = functions.correct_date_format(geo_dictionary)
        user_data = functions.correct_date_format(user_dictionary)

        put_api('streaming-0aa58e5ad07d-pin', pin_data)
        put_api('streaming-0aa58e5ad07d-geo', geo_data)
        put_api('streaming-0aa58e5ad07d-user', user_data)

if __name__ == "__main__":
    new_connector = sql_engine_connector.AWSDBConnector('Sensitive data/db_credentials.yaml')
    new_connector.read_db_creds()
    new_connector.init_db_engine()
    run()
    print('Working')