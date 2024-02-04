import requests
import json

example_df = {"index": 1, "name": "Maya", "age": 25, "role": "engineer"}

invoke_url = "https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/0aa58e5ad07d.geo/"
#To send JSON messages you need to follow this structure
payload = json.dumps({
    "records": [
        {
        #Data should be send as pairs of column_name:value, with different columns separated by commas       
        "value": {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}
        }
    ]
})

headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
response = requests.request("POST", invoke_url, headers=headers, data=payload)

print(payload)
print(response.status_code)
print(response.json())
print(type(payload))