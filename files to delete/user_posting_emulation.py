import requests
from time import sleep
import random
from multiprocessing import Process
import json
import sqlalchemy
from sqlalchemy import text
import pandas as pd

random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    #while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            pd_pin = pd.DataFrame(pin_selected_row)

            for row in pin_selected_row:
                pin_result = json.dumps(dict(row._mapping))

            # geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            # geo_selected_row = connection.execute(geo_string)
            
            # for row in geo_selected_row:
            #     geo_result = dict(row._mapping)

            # user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            # user_selected_row = connection.execute(user_string)
            
            # for row in user_selected_row:
            #     user_result = dict(row._mapping)
            
            invoke_url = "https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/0aa58e5ad07d.user"
            payload = json.dumps(pin_result)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            response = requests.request("POST", invoke_url, headers=headers, data=payload)

            print(response.status_code)

            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')