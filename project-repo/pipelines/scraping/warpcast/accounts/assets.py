# pipelines/scraping/warpcast/accounts/assets.py
from dagster import asset, Config, AssetObservation, MetadataValue
from datetime import datetime
import json
import boto3
import os
import time

# Import your scraper
from pipelines.scraping.warpcast.accounts.scrape import WarpcastAccountScraper

# Configuration
class WarpcastS3Config(Config):
    start_fid: int = 0
    end_fid: int = 867000
    bucket_name: str = "census-warpcast-account-metadata"
    aws_region: str = "us-east-1"
    batch_size: int = 100
    requests_per_minute: int = 30 # Default to 5 RPM to be safe
    max_retries: int = 1 # Maximum number of retries for failed requests

@asset
def warpcast_users_to_s3(context, config: WarpcastS3Config):
    """Asset that fetches Warpcast user data and stores it in S3"""
    # Calculate total FIDs to process
    total_fids = config.end_fid - config.start_fid + 1
    
    context.log.info(f"Starting to fetch data for {total_fids} FIDs ({config.start_fid} to {config.end_fid})")
    context.log.info(f"Rate limiting: {config.requests_per_minute} requests per minute, max retries: {config.max_retries}")
    
    # Create scraper instance with rate limiting
    scraper = WarpcastAccountScraper(requests_per_minute=config.requests_per_minute)
    
    # Store raw API responses
    responses = []
    successful_batches = 0
    failed_batches = 0
    total_users_found = 0
    
    # Process FIDs in batches
    batch_count = 0
    for i in range(config.start_fid, config.end_fid + 1, config.batch_size):
        batch_count += 1
        
        batch_end = min(i + config.batch_size, config.end_fid + 1)
        batch = list(range(i, batch_end))
        
        context.log.info(f"Processing batch {batch_count} with {len(batch)} FIDs (from {i} to {batch_end-1})")
        
        # Get the raw API response with retries
        retry_count = 0
        success = False
        response = None
        
        while retry_count <= config.max_retries and not success:
            if retry_count > 0:
                wait_time = 20  # Wait 20 seconds before retrying
                context.log.info(f"Retrying batch {batch_count} in {wait_time} seconds (attempt {retry_count}/{config.max_retries})")
                time.sleep(wait_time)
            
            response = scraper.query_neynar_api_for_users(batch)
            
            # Check if request was successful
            if response and not response.get('error'):
                success = True
            else:
                retry_count += 1
                context.log.error(f"Batch {batch_count} failed. Error: {json.dumps(response.get('error'))}")
        
        # Log response metadata
        if success and 'users' in response:
            users_count = len(response['users'])
            total_users_found += users_count
            successful_batches += 1
            
            context.log.info(f"Batch {batch_count} found {users_count} users (total so far: {total_users_found})")
            
            if users_count > 0:
                sample_count = min(3, users_count)
                sample = response['users'][:sample_count]
                context.log.info(f"Sample users: {json.dumps(sample)[:500]}...")
        else:
            failed_batches += 1
            context.log.error(f"Batch {batch_count} failed after {config.max_retries + 1} attempts")
        
        # Store the response with batch metadata
        responses.append({
            "batch": batch_count,
            "fid_range": [i, batch_end-1],
            "success": success,
            "users_found": users_count if success and 'users' in response else 0,
            "retry_count": retry_count,
            "data": response
        })
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        region_name=config.aws_region,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"warpcast/users/{timestamp}_responses.json"
    
    # Upload the raw responses
    try:
        s3_client.put_object(
            Bucket=config.bucket_name,
            Key=key,
            Body=json.dumps(responses),
            ContentType='application/json'
        )
        context.log.info(f"Successfully uploaded data to s3://{config.bucket_name}/{key}")
        
    except Exception as e:
        context.log.error(f"Error uploading to S3: {str(e)}")
        raise
    
    # Log summary
    context.log.info(f"SUMMARY: Processed {batch_count} batches, found {total_users_found} users total")
    context.log.info(f"Successful batches: {successful_batches}, Failed batches: {failed_batches}")
    
    # Add metadata about the operation
    metadata = {
        "start_fid": MetadataValue.int(config.start_fid),
        "end_fid": MetadataValue.int(config.end_fid),
        "batches_processed": MetadataValue.int(batch_count),
        "successful_batches": MetadataValue.int(successful_batches),
        "failed_batches": MetadataValue.int(failed_batches),
        "users_found": MetadataValue.int(total_users_found),
        "s3_path": MetadataValue.text(f"s3://{config.bucket_name}/{key}"),
        "timestamp": MetadataValue.text(datetime.now().isoformat())
    }
    
    context.log_event(
        AssetObservation(asset_key="warpcast_users_to_s3", metadata=metadata)
    )
    
    return {
        "batches_processed": batch_count,
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "users_found": total_users_found,
        "s3_path": f"s3://{config.bucket_name}/{key}"
    }