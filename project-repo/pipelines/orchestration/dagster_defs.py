# pipelines/orchestration/dagster_defs.py
from dagster import Definitions, job, asset

# Import all necessary assets and jobs from their modules
from pipelines.scraping.warpcast.accounts.assets import warpcast_users_to_s3
from pipelines.scraping.warpcast.accounts.scrape import WarpcastAccountScraper
# Import the new channel assets
from pipelines.scraping.warpcast.channels.assets import farcaster_channels_to_s3
from pipelines.scraping.warpcast.channels.scrape import FarcasterChannelScraper

@job
def warpcast_s3_job():
    """Job that loads Warpcast data into S3"""
    warpcast_users_to_s3()

@job
def farcaster_channels_s3_job():
    """Job that loads Farcaster channel data into S3"""
    farcaster_channels_to_s3()

# Single source of truth for all Dagster definitions
defs = Definitions(
    assets=[warpcast_users_to_s3, farcaster_channels_to_s3],
    jobs=[warpcast_s3_job, farcaster_channels_s3_job],
)