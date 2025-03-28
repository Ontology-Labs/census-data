from dagster import asset, AssetObservation, MetadataValue
from datetime import datetime
import time
from .ingest import WarpcastIngester

@asset
def warpcast_accounts_to_neo4j(context):
    """
    Asset that fetches Warpcast account data from S3 and loads it into Neo4j
    """
    context.log.info("Starting Warpcast account ingestion asset")
    start_time = time.time()
    
    # Initialize the ingester with context for logging
    ingester = WarpcastIngester(context=context)
    
    # Run ingestion pipeline
    if not ingester.run():
        context.log.error("Ingestion process failed")
        raise Exception("Ingestion process failed")
    
    # Calculate elapsed time
    elapsed = time.time() - start_time
    context.log.info(f"Completed Warpcast account ingestion in {elapsed:.2f} seconds")
    
    # Record observability metadata
    metadata = {
        "processing_time_seconds": MetadataValue.float(elapsed),
        "timestamp": MetadataValue.text(datetime.now().isoformat()),
    }
    
    context.log_event(
        AssetObservation(asset_key="warpcast_accounts_to_neo4j", metadata=metadata)
    )
    
    return {
        "success": True,
        "processing_time_seconds": elapsed,
        "timestamp": datetime.now().isoformat()
    }