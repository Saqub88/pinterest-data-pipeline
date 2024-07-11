"""
user_posting_emulation_batch.py

This program is designed to emulate user posting on Pinterest by using a batch
of predefined data from a database and random length pauses to replicate the
irregular timing  of real users posting content. This data is then sent to 
their respective Kafka topics via POST APIs for further processing.

To run this script there is a requirement that the Kafka server is running.
This can be done by following the instructions in the README.md file under the
'Usage instructions' heading and 'Batch processing' section.

This code is designed to run continuously without interruption. To stop this
code from running you will need to press Ctrl+C at the terminal.
"""

import sql_engine_connector
import functions

import requests
from time import sleep
import random
import json

random.seed(100)


def post_api(topic, input_dict):
    """
    A function to send data through an API to Kafka topics. It sends a POST
    request to the API server with the given topic and dictionary as a JSON
    payload.
    
    Args:
        topic (str): The name of the topic to be used in the URL of the API.
            It must match one of the stream names of the Kinesis data streams.
        input_dict (dictionary): A Python dictionary containing key-value pairs
            representing the data to send.
    """
    payload = json.dumps({ "records" : [{ "value" : input_dict }] })
    invoke_url = (
        "https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/"
    )
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
    response = requests.request(
        "POST", f"{invoke_url}{topic}", headers=headers, data=payload
    )
    print(response.status_code)
    print(response.json())


def run(engine):
    """
     This function takes in an SQLAlchemy engine object extracts data from the
     RDS instance randomly 1 row at a time from each of the three tables stored
     in the database. This data is then converted into dictionaries, formatted
     appropriately and then sent to their respective Kinesis Data Stream via
     the `post_api` function. This is repeated continuously at random time
     intervals to simulate live data being recieved.

     Args:
        engine (sqlalchemy.engine): An initialized sqlalchemy engine connected
            to a specific RDS instance.
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        pin_data = functions.extract_data_to_dictionary(
            engine, f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"
        )
        geo_data = functions.extract_data_to_dictionary(
            engine, f"SELECT * FROM geolocation_data LIMIT {random_row}, 1"
        )
        user_data = functions.extract_data_to_dictionary(
            engine, f"SELECT * FROM user_data LIMIT {random_row}, 1"
        )
        post_api("streaming-0aa58e5ad07d-pin", pin_data)
        post_api("streaming-0aa58e5ad07d-geo", geo_data)
        post_api("streaming-0aa58e5ad07d-user", user_data)


if __name__ == "__main__":
    # Initialise the engine that connects to the AWS RDS instance
    rds_connector = sql_engine_connector.AWSDBConnector(
        "sensitive_data/db_credentials.yaml"
    )
    # Extract and clean data and send it to the API
    run(rds_connector.engine)
