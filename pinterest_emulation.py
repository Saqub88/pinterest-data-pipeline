import requests
from time import sleep
import random
import json
import yaml
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

random.seed(100)

class AWSDBConnector:

    def __init__(self):
        
        self.database_credentials = None
        self.engine = None
    
    
    def read_db_creds(self, credential_file):
        with open(credential_file, 'r') as file:
            self.database_credentials = yaml.safe_load(file)

    
    def init_db_engine(self):
        self.engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.database_credentials['USER']}"
            f":{self.database_credentials['PASSWORD']}"
            f"@{self.database_credentials['HOST']}"
            f":{self.database_credentials['PORT']}"
            f"/{self.database_credentials['DATABASE']}?charset=utf8mb4"
        )

new_connector = AWSDBConnector()
new_connector.read_db_creds('Sensitive data/db_credentials.yaml')
new_connector.init_db_engine()

def function(sql_querry):
        with new_connector.engine.connect() as connection:
            pin_string = text(sql_querry)
            pin_table = connection.execute(pin_string)
            for row in pin_table:
                pin_raw_data = dict(row._mapping)
            for item in pin_raw_data:
                if type(pin_raw_data[item]) == datetime:
                    pin_raw_data[item] = (pin_raw_data[item]).isoformat()
                    print(pin_raw_data[item])
            return pin_raw_data

def post_api(topic, input_dict):
    payload = json.dumps({
        "records": [
            {
            #Data should be send as pairs of column_name:value, with different columns separated by commas       
            "value": input_dict
            }
        ]
    })
    invoke_url = "https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/"
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", f"{invoke_url}{topic}", headers=headers, data=payload)
    print(response.status_code)
    print(response.json())

def run():
#     while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        with new_connector.engine.connect() as connection:
            pin_result = function(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            geo_result = function(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            user_result = function(f"SELECT * FROM user_data LIMIT {random_row}, 1")

            print(pin_result)
            print(geo_result)
            print(user_result)

            post_api('0aa58e5ad07d.pin', pin_result)
            post_api('0aa58e5ad07d.geo', geo_result)
            post_api('0aa58e5ad07d.user', user_result)

run()