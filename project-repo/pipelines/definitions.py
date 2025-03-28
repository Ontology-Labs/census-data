from dagster import Definitions, load_assets_from_modules
from pipelines.scraping.warpcast.accounts.scrape import warpcast_job

# Combine definitions from all your pipelines
defs = Definitions(
    jobs=[warpcast_job],
)