# pipelines/scraping/warpcast/channels/assets.py

from dagster import asset, Config, AssetObservation, MetadataValue
from typing import List
from datetime import datetime
import json
import boto3
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Import your scraper
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
    """Asset that fetches Farcaster channel data and stores it in S3."""
    # Just directly load channel IDs from environment
    channel_ids_env = os.getenv('CHANNEL_IDS')
    
    # Try to parse the environment variable
    try:
        channel_ids = json.loads(channel_ids_env) if channel_ids_env else []
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
    context.log.info(
        f"Rate limiting: {config.requests_per_minute} requests per minute, "
        f"max retries: {config.max_retries}"
    )
    
    # Create scraper instance with rate limiting
    scraper = FarcasterChannelScraper(requests_per_minute=config.requests_per_minute)
    
    # Store channel data
    all_channel_data = {"channels": []}
    successful_channels = 0
    failed_channels = 0
    
    # -------------------------
    # HELPER: attempt a request
    # -------------------------
    def attempt_request(fetch_fn, *fetch_args, **fetch_kwargs):
        """
        Calls a fetch_fn like get_channel_metadata. Returns (success, data_or_err).
         - success: bool, whether we treat it as success
         - data_or_err: the raw response or an error explanation
        We consider it success if the returned data is not None and does not have an "error" key
        (for dictionaries).
        """
        response = fetch_fn(*fetch_args, **fetch_kwargs)
        
        if response is None:
            return False, None
        
        # If it's a dict with an "error" key, treat that as a failure
        if isinstance(response, dict) and "error" in response:
            return False, response
        
        # Otherwise, success
        return True, response

    # -------------------------
    # MAIN LOOP
    # -------------------------
    for channel_id in channel_ids:
        context.log.info(f"Processing channel ID: {channel_id}")
        
        channel_dict = {
            "channel_id": channel_id,
            "success": False,
            "errors": []
        }
        
        # 1) Get channel metadata with retries
        metadata = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20  # Wait 20 seconds before retrying
                context.log.info(
                    f"Retrying channel metadata for {channel_id} in {wait_time}s "
                    f"(attempt {retry_count}/{config.max_retries})"
                )
                time.sleep(wait_time)
            
            success, meta_or_err = attempt_request(scraper.get_channel_metadata, channel_id)
            
            if success:
                metadata = meta_or_err
            else:
                retry_count += 1
                error_msg = (
                    f"Failed to get metadata for channel {channel_id}. "
                    f"Response was: {meta_or_err}"
                )
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # If we got metadata, store raw
        if metadata is not None:
            channel_dict["metadata"] = metadata

        # 2) Get followers
        followers = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(
                    f"Retrying channel followers for {channel_id} in {wait_time}s "
                    f"(attempt {retry_count}/{config.max_retries})"
                )
                time.sleep(wait_time)
            
            success, foll_or_err = attempt_request(scraper.get_channel_followers, channel_id)
            
            if success:
                followers = foll_or_err
            else:
                retry_count += 1
                error_msg = (
                    f"Failed to get followers for channel {channel_id}. "
                    f"Response was: {foll_or_err}"
                )
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # Store raw response
        if followers is not None:
            channel_dict["followers"] = followers
            # If it's a dict with a "followers" key, log the count
            if isinstance(followers, dict):
                num_followers = len(followers.get("followers", []))
                context.log.info(f"Found {num_followers} followers for channel {channel_id}")

        # 3) Get members
        members = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(
                    f"Retrying channel members for {channel_id} in {wait_time}s "
                    f"(attempt {retry_count}/{config.max_retries})"
                )
                time.sleep(wait_time)
            
            success, mem_or_err = attempt_request(scraper.get_channel_members, channel_id)
            
            if success:
                members = mem_or_err
            else:
                retry_count += 1
                error_msg = (
                    f"Failed to get members for channel {channel_id}. "
                    f"Response was: {mem_or_err}"
                )
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)

        # Store raw response
        if members is not None:
            channel_dict["members"] = members
            # If it's a dict with a "members" key, log the count
            if isinstance(members, dict):
                num_members = len(members.get("members", []))
                context.log.info(f"Found {num_members} members for channel {channel_id}")

        # 4) Get casts
        casts = None
        retry_count = 0
        success = False
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20
                context.log.info(
                    f"Retrying channel casts for {channel_id} in {wait_time}s "
                    f"(attempt {retry_count}/{config.max_retries})"
                )
                time.sleep(wait_time)
            
            success, casts_or_err = attempt_request(
                scraper.get_channel_casts, channel_id, cutoff_days=config.cutoff_days
            )
            
            if success:
                casts = casts_or_err
            else:
                retry_count += 1
                error_msg = (
                    f"Failed to get casts for channel {channel_id}. "
                    f"Response was: {casts_or_err}"
                )
                context.log.error(error_msg)
                channel_dict["errors"].append(error_msg)
        
        # Store raw response
        if casts is not None:
            channel_dict["casts"] = casts
            # If it's a dict with "casts", log length
            if isinstance(casts, dict):
                num_casts = len(casts.get("casts", []))
                context.log.info(f"Found {num_casts} casts for channel {channel_id}")
        
        # Mark channel as successful if we got *any* data
        if any(
            key in channel_dict
            for key in ["metadata", "followers", "members", "casts"]
        ):
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
    meta_info = {
        "channels_processed": MetadataValue.int(total_channels),
        "successful_channels": MetadataValue.int(successful_channels),
        "failed_channels": MetadataValue.int(failed_channels),
        "s3_path": MetadataValue.text(f"s3://{config.bucket_name}/{key}"),
        "timestamp": MetadataValue.text(datetime.now().isoformat())
    }
    
    context.log_event(
        AssetObservation(asset_key="farcaster_channels_to_s3", metadata=meta_info)
    )
    
    return {
        "channels_processed": total_channels,
        "successful_channels": successful_channels,
        "failed_channels": failed_channels,
        "s3_path": f"s3://{config.bucket_name}/{key}"
    }
