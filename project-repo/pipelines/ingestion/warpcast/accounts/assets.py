from dagster import asset, AssetObservation, Config, MetadataValue
from datetime import datetime
import time
import logging
from .ingest import WarpcastIngester

# Configuration class for the asset
class WarpcastIngestConfig(Config):
    batch_size: int = 1000
    test_mode: bool = False
    test_limit: int = 100

@asset
def warpcast_accounts_to_neo4j(context, config: WarpcastIngestConfig):
    """
    Asset that fetches Warpcast account data from S3 and loads it into Neo4j
    """
    context.log.info("Starting Warpcast account ingestion asset")
    start_time = time.time()
    
    # Initialize the ingester with config
    ingester = WarpcastIngester(
        batch_size=config.batch_size,
        test_mode=config.test_mode
    )
    
    # If in test mode, override the test limit
    if config.test_mode:
        ingester.test_limit = config.test_limit
        context.log.info(f"Test mode enabled: limiting to {config.test_limit} records")
    
    # Load data from S3
    context.log.info("Loading data from S3")
    if not ingester.load_data():
        raise Exception("Failed to load data from S3")
    
    # Process the raw data
    context.log.info("Processing account data")
    if not ingester.process_account_data():
        raise Exception("Failed to process account data")
    
    # Log statistics about the data
    record_count = len(ingester.users_df)
    context.log.info(f"Processed {record_count} user records")
    
    # Process in batches
    context.log.info(f"Processing {record_count} users in Neo4j in batches of {config.batch_size}")
    if not ingester.process_in_batches():
        raise Exception("Failed to process batches")
    
    # Calculate elapsed time
    elapsed = time.time() - start_time
    context.log.info(f"Completed Warpcast account ingestion in {elapsed:.2f} seconds")
    
    # Record observability metadata
    metadata = {
        "record_count": MetadataValue.int(record_count),
        "processing_time_seconds": MetadataValue.float(elapsed),
        "batch_size": MetadataValue.int(config.batch_size),
        "test_mode": MetadataValue.bool(config.test_mode),
        "timestamp": MetadataValue.text(datetime.now().isoformat()),
    }
    
    context.log_event(
        AssetObservation(asset_key="warpcast_accounts_to_neo4j", metadata=metadata)
    )
    
    return {
        "record_count": record_count,
        "processing_time_seconds": elapsed,
        "timestamp": datetime.now().isoformat()
    }