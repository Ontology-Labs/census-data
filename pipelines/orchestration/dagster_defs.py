# pipelines/orchestration/dagster_defs.py
from dagster import Definitions, job

# Import assets from their own modules
from pipelines.scraping.warpcast.accounts.assets import warpcast_users_to_s3

@job
def warpcast_s3_job():
    """Job that loads Warpcast data into S3"""
    warpcast_users_to_s3()

# Dagster definitions
defs = Definitions(
    assets=[warpcast_users_to_s3],
    jobs=[warpcast_s3_job],
)