# pipelines/scraping/warpcast/channels/assets.py
from dagster import asset, Config, AssetObservation, MetadataValue
from typing import List
from datetime import datetime
import json
import boto3
import os
import time

# Import your scraper
from pipelines.scraping.warpcast.channels.scrape import FarcasterChannelScraper

# Configuration
class FarcasterChannelsS3Config(Config):
    channel_ids: List[str] = []
    bucket_name: str = "census-farcaster-channel-data"
    aws_region: str = "us-east-1"
    requests_per_minute: int = 45
    max_retries: int = 1
    cutoff_days: int = 1000
    
    # Load channel IDs from environment during validation
    @classmethod
    def model_validate(cls, data, *args, **kwargs):
        if not data.get("channel_ids"):
            channel_ids_env = os.getenv('CHANNEL_IDS')
            if channel_ids_env:
                try:
                    data["channel_ids"] = json.loads(channel_ids_env)
                except json.JSONDecodeError:
                    pass
        return super().model_validate(data, *args, **kwargs)

@asset
def farcaster_channels_to_s3(context, config: FarcasterChannelsS3Config):
    """Asset that fetches Farcaster channel data and stores it in S3"""
    # Check if we have any channel IDs to process
    total_channels = len(config.channel_ids)
    if not total_channels:
        context.log.error("No channel IDs provided - nothing to process")
        return {
            "channels_processed": 0,
            "error": "No channel IDs provided"
        }
    
    context.log.info(f"Starting to fetch data for {total_channels} channels: {config.channel_ids}")
    context.log.info(f"Rate limiting: {config.requests_per_minute} requests per minute, max retries: {config.max_retries}")
    
    # Create scraper instance with rate limiting
    scraper = FarcasterChannelScraper(requests_per_minute=config.requests_per_minute)
    
    # Store channel data
    all_channel_data = {"channels": []}
    successful_channels = 0
    failed_channels = 0
    
    # Process each channel
    for channel_id in config.channel_ids:
        context.log.info(f"Processing channel ID: {channel_id}")
        
        channel_dict = {
            "channel_id": channel_id,
            "success": False,
            "errors": []
        }
        
        # Get channel metadata with retries
        metadata = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20  # Wait 20 seconds before retrying
                context.log.info(f"Retrying channel metadata for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            metadata = scraper.get_channel_metadata(channel_id)
            
            # Check if request was successful
            if metadata and not metadata.get('error'):
                success = True
                channel_dict["metadata"] = metadata
            else:
                retry_count += 1
                error_msg = f"Failed to get metadata for channel {channel_id}: {json.dumps(metadata.get('error', 'Unknown error'))}"
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # Get followers
        followers = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(f"Retrying channel followers for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            followers = scraper.get_channel_followers(channel_id)
            
            if followers and not followers.get('error'):
                success = True
                channel_dict["followers"] = followers
                context.log.info(f"Found {len(followers.get('followers', []))} followers for channel {channel_id}")
            else:
                retry_count += 1
                error_msg = f"Failed to get followers for channel {channel_id}: {json.dumps(followers.get('error', 'Unknown error'))}"
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # Get members
        members = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(f"Retrying channel members for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            members = scraper.get_channel_members(channel_id)
            
            if members and not members.get('error'):
                success = True
                channel_dict["members"] = members
                context.log.info(f"Found {len(members.get('members', []))} members for channel {channel_id}")
            else:
                retry_count += 1
                error_msg = f"Failed to get members for channel {channel_id}: {json.dumps(members.get('error', 'Unknown error'))}"
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # Get casts
        casts = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(f"Retrying channel casts for {channel_id} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            casts = scraper.get_channel_casts(channel_id, cutoff_days=config.cutoff_days)
            
            if casts and not casts.get('error'):
                success = True
                channel_dict["casts"] = casts
                context.log.info(f"Found {len(casts.get('casts', []))} casts for channel {channel_id}")
            else:
                retry_count += 1
                error_msg = f"Failed to get casts for channel {channel_id}: {json.dumps(casts.get('error', 'Unknown error'))}"
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # Mark channel as successful if we have at least some data
        if "metadata" in channel_dict or "followers" in channel_dict or "members" in channel_dict or "casts" in channel_dict:
            channel_dict["success"] = True
            successful_channels += 1
        else:
            failed_channels += 1
        
        # Add channel data to result
        all_channel_data["channels"].append(channel_dict)
    
    # Initialize S3 client
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
    
    return {
        "channels_processed": total_channels,
        "successful_channels": successful_channels,
        "failed_channels": failed_channels,
        "s3_path": f"s3://{config.bucket_name}/{key}"
    }