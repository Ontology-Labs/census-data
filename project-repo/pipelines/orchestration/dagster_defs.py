# pipelines/orchestration/dagster_defs.py
from dagster import Definitions, job, asset

# Import all necessary assets and jobs from their modules
from pipelines.scraping.warpcast.accounts.assets import warpcast_users_to_s3
from pipelines.scraping.warpcast.accounts.scrape import WarpcastAccountScraper
# Import the channel assets
from pipelines.scraping.warpcast.channels.assets import farcaster_channels_to_s3
from pipelines.scraping.warpcast.channels.scrape import FarcasterChannelScraper
# Import the Neo4j ingest asset
from pipelines.ingestion.warpcast.accounts.assets import warpcast_accounts_to_neo4j

@job
def warpcast_s3_job():
    """Job that loads Warpcast data into S3"""
    warpcast_users_to_s3()

@job
def farcaster_channels_s3_job():
    """Job that loads Farcaster channel data into S3"""
    farcaster_channels_to_s3()

@job
def warpcast_neo4j_job():
    """Job that loads Warpcast account data into Neo4j"""
    warpcast_accounts_to_neo4j()

# Single source of truth for all Dagster definitions
defs = Definitions(
    assets=[
        warpcast_users_to_s3, 
        farcaster_channels_to_s3, 
        warpcast_accounts_to_neo4j
    ],
    jobs=[
        warpcast_s3_job, 
        farcaster_channels_s3_job, 
        warpcast_neo4j_job
    ],
)