import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


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


def run_infinite_post_data_loop():
    random_row = random.randint(0, 11000)

    with new_connector.engine.connect() as connection:

        pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
        pin_selected_row = connection.execute(pin_string)
        
        for row in pin_selected_row:
            pin_result = dict(row._mapping)

        geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
        geo_selected_row = connection.execute(geo_string)
        
        for row in geo_selected_row:
            geo_result = dict(row._mapping)

        user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
        user_selected_row = connection.execute(user_string)
        
        for row in user_selected_row:
            user_result = dict(row._mapping)

        invoke_url = "https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/0aa58e5ad07d.user"
        payload = json.dumps(pin_result)
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        response = requests.request("POST", invoke_url, headers=headers, data=payload)

        print(response.status_code)
        print(response.json())

        print(pin_result)
        print(geo_result)
        print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')