from datetime import datetime, timezone, timedelta
import json
import logging
import time
import requests as r
from requests import RequestException
from botocore.exceptions import ClientError

def save_data(s3_client, bucket_name, key, data):
    """Saves data to S3"""
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
        logging.info(f"Data saved to {key}")
    except ClientError as e:
        logging.error(f"Failed to save data: {e}")

def query_neynar_hub(endpoint, params=None):
    """
    Legacy function for querying the Hub API.
    """
    base_url = "https://hub-api.neynar.com/v1/"
    headers = {
        "Content-Type": "application/json"
    }
    url = f"{base_url}{endpoint}"
    params = params or {}
    params['pageSize'] = 1000

    all_messages = []
    max_retries = 3
    retry_delay = 1

    while True:
        for attempt in range(max_retries):
            try:
                time.sleep(0.1)
                response = r.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                if 'messages' in data:
                    for message in data['messages']:
                        if 'timestamp' in message.get('data', {}):
                            all_messages.extend(data['messages'])
                            print(f"Retrieved {len(all_messages)} messages total...")
                
                if 'nextPageToken' in data and data['nextPageToken']:
                    params['pageToken'] = data['nextPageToken']
                    break
                else:
                    return all_messages
                
            except RequestException as e:
                if attempt == max_retries - 1:
                    print(f"Failed after {max_retries} attempts. Error: {e}")
                    return all_messages
                else:
                    print(f"Attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2