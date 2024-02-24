import requests

response = requests.post("https://m1c8pv5ln1.execute-api.us-east-1.amazonaws.com/test/topics/0aa58e5ad07d.pin")
print(response.json())
print(response.status_code)