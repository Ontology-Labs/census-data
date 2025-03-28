from datetime import datetime, timezone, timedelta
import json
import logging
import time
import requests as r
from requests import RequestException
from botocore.exceptions import ClientError


### ADD A TIMESTAMP/HASH/LIMIT LOGIC AND HTEN AN "EVERYTHING" OPTION.

# def convert_timestamp(timestamp, epoch):
#     """Converts Farcaster Hub Timestamp to UTC datetime"""
#     return epoch + timedelta(seconds=int(timestamp))

def save_data(s3_client, bucket_name, key, data):
    """Saves data to S3"""
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(data))
        logging.info(f"Data saved to {key}")
    except ClientError as e:
        logging.error(f"Failed to save data: {e}")

    
def query_neynar_hub(endpoint, params=None):
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


# helpers.py
def query_neynar_api(endpoint, params, headers, cutoff_days=None):
    base_url = "https://api.neynar.com/v2/farcaster/"
    url = f"{base_url}{endpoint}"
    all_data = []
    reached_cutoff = False
    
    current_params = params.copy()
    logging.info(f"Retrieving data from {endpoint}...")
    
    # Calculate cutoff date if provided
    if cutoff_days is not None:
        cutoff_days = int(cutoff_days)  # Convert to integer
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
        logging.info(f"Cutoff date set to: {cutoff_date}")
    
    while True and not reached_cutoff:
        try:
            response = r.get(url, headers=headers, params=current_params)
            response.raise_for_status()
            data = response.json()
            
            first_key = next(iter(data), None)
            if first_key is None:
                break
                
            extracted_data = data.get(first_key, [])
            logging.info(f"Retrieved batch of {len(extracted_data)} items")
            
            # Check temporal cutoff if applicable
            if cutoff_days is not None:
                current_batch = []
                for item in extracted_data:
                    timestamp_str = item.get('timestamp')
                    if timestamp_str:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        if timestamp >= cutoff_date:
                            current_batch.append(item)
                        else:
                            reached_cutoff = True
                
                if current_batch:
                    all_data.extend(current_batch)
                    logging.info(f"Added {len(current_batch)} items within cutoff period")
                
                if reached_cutoff:
                    logging.info(f"Reached cutoff date, stopping pagination")
                    break
            else:
                all_data.extend(extracted_data)
            
            logging.info(f"Total items collected: {len(all_data)}")
            
            next_info = data.get('next', {})
            cursor = next_info.get('cursor')
            
            if not cursor:
                break
                
            current_params['cursor'] = cursor
            
        except r.RequestException as e:
            if hasattr(e.response, 'status_code'):
                if e.response.status_code == 400:
                    print(f"Bad request. Response content: {e.response.text[:200]}")
                elif e.response.status_code == 404:
                    print("Endpoint not found. Exiting loop.")
            break
    
    logging.info(f"Final count of items: {len(all_data)}")
    return all_data