# pipelines/scraping/warpcast/channels/assets.py

from dagster import asset, Config, AssetObservation
from dagster.core.definitions.metadata import MetadataValue
from typing import List
from datetime import datetime
import json
import boto3
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import project-specific modules
from pipelines.scraping.warpcast.channels.scrape import FarcasterChannelScraper

# Configuration
class FarcasterChannelsS3Config(Config):
    channel_ids: List[str] = []  # Will be populated from env in the asset function
    bucket_name: str = "census-farcaster-channel-data"
    aws_region: str = "us-east-1"
    requests_per_minute: int = 45
    max_retries: int = 1
    cutoff_days: int = 1000

@asset
def farcaster_channels_to_s3(context, config: FarcasterChannelsS3Config):
    """Asset that fetches Farcaster channel data and stores it in S3"""
    # Just directly load channel IDs from environment
    context.log.info("Starting farcaster_channels_to_s3 asset function")
    channel_ids_env = os.getenv('CHANNEL_IDS')
    
    # Try to parse the environment variable
    try:
        channel_ids = json.loads(channel_ids_env) if channel_ids_env else []
        context.log.info(f"Successfully parsed channel IDs from environment")
    except Exception as e:
        context.log.error(f"Error parsing CHANNEL_IDS: {str(e)}")
        channel_ids = []
        
    # Log what we loaded
    context.log.info(f"Loaded channel IDs from environment: {channel_ids}")
    
    # Check if we have any channel IDs to process
    total_channels = len(channel_ids)
    if not total_channels:
        context.log.error("No channel IDs provided - nothing to process")
        return {
            "channels_processed": 0,
            "error": "No channel IDs provided"
        }
    
    context.log.info(f"Starting to fetch data for {total_channels} channels: {channel_ids}")
    context.log.info(f"Rate limiting: {config.requests_per_minute} requests per minute, max retries: {config.max_retries}")
    
    # Create scraper instance with rate limiting
    context.log.info("Initializing FarcasterChannelScraper")
    scraper = FarcasterChannelScraper(requests_per_minute=config.requests_per_minute)
    
    # Store channel data
    all_channel_data = {"channels": []}
    successful_channels = 0
    failed_channels = 0
    
    # Process each channel
    for idx, channel_id in enumerate(channel_ids, 1):
        context.log.info(f"[{idx}/{total_channels}] Processing channel ID: {channel_id}")
        
        channel_dict = {
            "channel_id": channel_id,
            "success": False,
            "errors": []
        }
        
        # Get channel metadata with retries
        context.log.info(f"[{idx}/{total_channels}] Starting metadata fetch for channel: {channel_id}")
        metadata = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20  # Wait 20 seconds before retrying
                context.log.info(f"[{idx}/{total_channels}] Retrying channel metadata for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            context.log.info(f"[{idx}/{total_channels}] Calling get_channel_metadata for {channel_id}")
            metadata = scraper.get_channel_metadata(channel_id)
            context.log.info(f"[{idx}/{total_channels}] Received metadata response for {channel_id}")
            
            # Check if request was successful - looking for "channel" in response
            if metadata and "channel" in metadata:
                channel_dict["metadata"] = metadata
                success = True
                channel_name = metadata.get("channel", {}).get("name", "unknown")
                follower_count = metadata.get("channel", {}).get("follower_count", 0)
                context.log.info(f"[{idx}/{total_channels}] Successfully retrieved metadata for channel {channel_id} - Name: {channel_name}, Followers: {follower_count}")
            else:
                retry_count += 1
                error_msg = f"Failed to get metadata for channel {channel_id}"
                if metadata and "error" in metadata:
                    error_msg += f": {json.dumps(metadata['error'])}"
                context.log.error(f"[{idx}/{total_channels}] {error_msg}")
                channel_dict["errors"].append(error_msg)
        
        # Get followers
        context.log.info(f"[{idx}/{total_channels}] Starting followers fetch for channel: {channel_id}")
        followers = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(f"[{idx}/{total_channels}] Retrying channel followers for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            context.log.info(f"[{idx}/{total_channels}] Calling get_channel_followers for {channel_id}")
            followers = scraper.get_channel_followers(channel_id)
            context.log.info(f"[{idx}/{total_channels}] Received followers response for {channel_id}")
            
            if followers and "followers" in followers:
                success = True
                channel_dict["followers"] = followers
                context.log.info(f"[{idx}/{total_channels}] Found {len(followers.get('followers', []))} followers for channel {channel_id}")
            else:
                retry_count += 1
                error_msg = f"Failed to get followers for channel {channel_id}"
                if followers and "error" in followers:
                    error_msg += f": {json.dumps(followers['error'])}"
                context.log.error(f"[{idx}/{total_channels}] {error_msg}")
                channel_dict["errors"].append(error_msg)
        
        # Get members
        context.log.info(f"[{idx}/{total_channels}] Starting members fetch for channel: {channel_id}")
        members = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(f"[{idx}/{total_channels}] Retrying channel members for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            context.log.info(f"[{idx}/{total_channels}] Calling get_channel_members for {channel_id}")
            members = scraper.get_channel_members(channel_id)
            context.log.info(f"[{idx}/{total_channels}] Received members response for {channel_id}")
            
            if members and "members" in members:
                success = True
                channel_dict["members"] = members
                context.log.info(f"[{idx}/{total_channels}] Found {len(members.get('members', []))} members for channel {channel_id}")
            else:
                retry_count += 1
                error_msg = f"Failed to get members for channel {channel_id}"
                if members and "error" in members:
                    error_msg += f": {json.dumps(members['error'])}"
                context.log.error(f"[{idx}/{total_channels}] {error_msg}")
                channel_dict["errors"].append(error_msg)
        
        # Get casts
        context.log.info(f"[{idx}/{total_channels}] Starting casts fetch for channel: {channel_id}")
        casts = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(f"[{idx}/{total_channels}] Retrying channel casts for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            context.log.info(f"[{idx}/{total_channels}] Calling get_channel_casts for {channel_id} with cutoff_days={config.cutoff_days}")
            casts = scraper.get_channel_casts(channel_id, cutoff_days=config.cutoff_days)
            context.log.info(f"[{idx}/{total_channels}] Received casts response for {channel_id}")
            
            if casts and "casts" in casts:
                success = True
                channel_dict["casts"] = casts
                context.log.info(f"[{idx}/{total_channels}] Found {len(casts.get('casts', []))} casts for channel {channel_id}")
            else:
                retry_count += 1
                error_msg = f"Failed to get casts for channel {channel_id}"
                if casts and "error" in casts:
                    error_msg += f": {json.dumps(casts['error'])}"
                context.log.error(f"[{idx}/{total_channels}] {error_msg}")
                channel_dict["errors"].append(error_msg)
        
        # Mark channel as successful if we have at least some data
        if "metadata" in channel_dict or "followers" in channel_dict or "members" in channel_dict or "casts" in channel_dict:
            channel_dict["success"] = True
            successful_channels += 1
            context.log.info(f"[{idx}/{total_channels}] Channel {channel_id} processing completed successfully")
        else:
            failed_channels += 1
            context.log.error(f"[{idx}/{total_channels}] Channel {channel_id} processing failed completely")
        
        # Add channel data to result
        all_channel_data["channels"].append(channel_dict)
        context.log.info(f"[{idx}/{total_channels}] Channel {channel_id} data added to results")
    
    # Initialize S3 client
    context.log.info("Initializing S3 client for data upload")
    s3_client = boto3.client(
        's3',
        region_name=config.aws_region,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"farcaster/channels/{timestamp}_channel_data.json"
    
    # Upload the data
    try:
        context.log.info(f"Uploading data to S3 bucket: {config.bucket_name}, key: {key}")
        s3_client.put_object(
            Bucket=config.bucket_name,
            Key=key,
            Body=json.dumps(all_channel_data),
            ContentType='application/json'
        )
        context.log.info(f"Successfully uploaded data to s3://{config.bucket_name}/{key}")
        
    except Exception as e:
        context.log.error(f"Error uploading to S3: {str(e)}")
        raise
    
    # Log summary
    context.log.info(f"SUMMARY: Processed {total_channels} channels")
    context.log.info(f"Successful channels: {successful_channels}, Failed channels: {failed_channels}")
    
    # Add metadata about the operation
    context.log.info("Adding metadata to asset observation")
    metadata = {
        "channels_processed": MetadataValue.int(total_channels),
        "successful_channels": MetadataValue.int(successful_channels),
        "failed_channels": MetadataValue.int(failed_channels),
        "s3_path": MetadataValue.text(f"s3://{config.bucket_name}/{key}"),
        "timestamp": MetadataValue.text(datetime.now().isoformat())
    }
    
    context.log_event(
        AssetObservation(asset_key="farcaster_channels_to_s3", metadata=metadata)
    )
    
    context.log.info("Asset function completed")
    return {
        "channels_processed": total_channels,
        "successful_channels": successful_channels,
        "failed_channels": failed_channels,
        "s3_path": f"s3://{config.bucket_name}/{key}"
    }