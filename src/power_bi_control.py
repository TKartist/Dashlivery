import requests
import pandas as pd
import os
import json
from dotenv import load_dotenv
import VARIABLE
import time

load_dotenv()


def send_request(data, api_url, iteration):
    res = requests.post(api_url, headers=VARIABLE.HEADER, data=json.dumps(data))

    if res.status_code == 200:
        print(f"Batch {iteration}, uploaded")
    elif res.status_code == 429:
        print(f"Too many requests")
    else:
        print(f"Error {res.status_code}: {res.text}")
    
    print("Uploaded")



def batch_request(df, api_url):
    num_rows = len(df)
    req_count = num_rows // VARIABLE.REQUEST_LIMIT + 1
    prod = df.to_dict(orient="records")
    for i in range(req_count):
        send_request(prod[i * VARIABLE.REQUEST_LIMIT : max(num_rows, (i + 1) * VARIABLE.REQUEST_LIMIT)], api_url, (i + 1))
        remainder = (i + 1) % VARIABLE.RATE_LIMIT
        if remainder == 0:
            print("Waiting for REQUEST LIMIT to be refreshed")
            time.sleep(60)
        
    print("Finished Batched Uploading")
            


def upload_request(df, type):
    api_url = os.getenv(type)
    num_rows = len(df)
    if num_rows == 0:
        print("No information to upload passed")
        return
    elif num_rows > 500:
        batch_request(df, api_url)
    else:
        prod = df.to_dict(orient="records")
        send_request(prod, api_url, 1)


def delete_request(type):
    api_url = os.getenv(type)
    token = os.getenv(VARIABLE.ACCESS_TOKEN)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    res = requests.delete(api_url, headers=headers)

    if res.status_code == 200:
        print("The data is completely wiped")
    else:
        print(f"Error {res.status_code}: {res.text}")


def put_request(df, type):
    api_url = os.getenv(type)
    delete_request(type)
    upload_request(df, type)
    print("PowerBI Streaming Dataset Content Updated.")