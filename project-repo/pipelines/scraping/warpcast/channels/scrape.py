# pipelines/scraping/warpcast/channels/scrape.py

import os
import time
import json
import requests as r
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from .helpers  # Adjust import if needed

load_dotenv(override=True)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FarcasterChannelScraper:
    def __init__(self, requests_per_minute=45):
        """
        Initialize the FarcasterChannelScraper with a certain rate limit.
        requests_per_minute: how many requests per minute to allow.
        """
        self.NEYNAR_API_KEY = os.getenv('NEYNAR_API_KEY')
        self.FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.last_request_time = time.time()
        
        # Rate limiting
        self.request_delay = (60.0 / requests_per_minute) + 0.2
        logger.info(
            f"Rate limiting set to {requests_per_minute} requests/minute "
            f"(delay: {self.request_delay:.2f}s)"
        )

    def apply_rate_limit(self):
        """
        Ensures we don't exceed the specified request rate.
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _handle_request_exception(self, e):
        """
        Helper to handle request exceptions, logs status code and returns an error dict.
        """
        logger.error(f"Request error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status code: {e.response.status_code}")
            logger.error(f"Response content: {e.response.content}")
            try:
                return {"error": e.response.json()}
            except:
                pass
        return {"error": str(e)}

    def _parse_cast_timestamp(self, timestamp_str):
        """
        Handle both numeric offsets and ISO8601 timestamps from the Neynar API.
        Returns a Python datetime in UTC.
        """
        # Try interpreting timestamp_str as an integer offset from FARCASTER_EPOCH
        try:
            # e.g. '7200' means 7200 seconds after 2021-01-01T00:00:00Z
            offset_seconds = int(timestamp_str)
            return self.FARCASTER_EPOCH + timedelta(seconds=offset_seconds)
        except (TypeError, ValueError):
            # Fall back to parsing as an ISO8601 date string
            # e.g. '2025-03-20T16:12:35.000Z'
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            return datetime.fromisoformat(timestamp_str)

    def get_channel_metadata(self, channel_name):
        """
        Fetch channel metadata by channel ID (or 'channel_name') from the Neynar API.
        Uses the helpers.query_neynar_api function.
        """
        self.apply_rate_limit()  # apply rate limit before calling the API

        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        params = {
            'id': channel_name
        }
        endpoint = 'channel'

        try:
            logger.info(f"Fetching channel metadata for: {channel_name}")
            channel_metadata = helpers.query_neynar_api(endpoint, params, headers)
            return channel_metadata
        except r.RequestException as e:
            logging.error(f"Error fetching channel metadata: {e}")
            return None

    def get_channel_followers(self, channel_id, limit=1000):
        """
        Get followers for a specific channel from the Neynar API.
        """
        self.apply_rate_limit()
        
        base_url = "https://api.neynar.com/v2/farcaster/channel/followers"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "id": channel_id,
            "limit": limit
        }
        
        try:
            logger.info(f"Querying Neynar API for channel followers: {channel_id}")
            response = r.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data
        except r.RequestException as e:
            logger.error(f"Error querying Neynar API for channel followers: {e}")
            return self._handle_request_exception(e)

    def get_channel_members(self, channel_id, limit=100):
        """
        Get members for a specific channel from the Neynar API.
        """
        self.apply_rate_limit()

        base_url = "https://api.neynar.com/v2/farcaster/channel/member/list"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "channel_id": channel_id,
            "limit": limit
        }

        try:
            logger.info(f"Querying Neynar API for channel members: {channel_id}")
            response = r.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            return data
        except r.RequestException as e:
            logger.error(f"Error querying Neynar API for channel members: {e}")
            return self._handle_request_exception(e)

    def get_channel_casts(self, channel_id, limit=100, with_replies=True, cutoff_days=None):
        """
        Get casts for a specific channel from the Neynar API, optionally filtering
        out older casts via 'cutoff_days'.
        """
        self.apply_rate_limit()

        base_url = "https://api.neynar.com/v2/farcaster/feed/channels"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "channel_ids": channel_id,
            "limit": limit,
            "with_replies": with_replies
        }

        try:
            logger.info(f"Querying Neynar API for channel casts: {channel_id}")
            response = r.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # If there's no "casts" key, just return whatever we got
            if "casts" not in data:
                return data

            if cutoff_days:
                # Filter out casts older than now - cutoff_days
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
                filtered_casts = []
                for cast in data["casts"]:
                    cast_dt = self._parse_cast_timestamp(cast["timestamp"])
                    if cast_dt >= cutoff_dt:
                        filtered_casts.append(cast)
                data["casts"] = filtered_casts

            return data

        except r.RequestException as e:
            logger.error(f"Error querying Neynar API for channel casts: {e}")
            return self._handle_request_exception(e)
