# pipelines/scraping/warpcast/channels/scrape.py
import os
import time
import json
import requests
from requests import RequestException
from dotenv import load_dotenv
import logging
from datetime import datetime, timezone, timedelta

# Load environment variables
load_dotenv(override=True)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FarcasterChannelScraper:
    def __init__(self, requests_per_minute=45):
        self.NEYNAR_API_KEY = os.getenv('NEYNAR_API_KEY')
        self.FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.last_request_time = time.time()
        
        # Rate limiting
        self.request_delay = (60.0 / requests_per_minute) + 0.2
        logger.info(f"Rate limiting set to {requests_per_minute} requests per minute (delay: {self.request_delay:.2f}s)")

    def convert_timestamp(self, timestamp):
        """Convert Farcaster timestamp to UTC datetime."""
        dt = self.FARCASTER_EPOCH + timedelta(seconds=int(timestamp))
        return dt.timestamp()  # Return Unix timestamp as float
    
    def apply_rate_limit(self):
        """Apply rate limiting between API calls"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_channel_metadata(self, channel_id):
        """Get metadata for a specific channel"""
        base_url = "https://api.neynar.com/v2/farcaster/channel"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "id": channel_id
        }
        
        self.apply_rate_limit()
        
        try:
            logger.info(f"Querying Neynar API for channel metadata: {channel_id}")
            response = requests.get(base_url, headers=headers, params=params)
            
            response.raise_for_status()
            data = response.json()
            
            return data
        except RequestException as e:
            logger.error(f"Error querying Neynar API for channel metadata: {e}")
            return self._handle_request_exception(e)
    
    def get_channel_followers(self, channel_id, limit=1000):
        """Get followers for a specific channel"""
        base_url = "https://api.neynar.com/v2/farcaster/channel/followers"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "id": channel_id,
            "limit": limit
        }
        
        self.apply_rate_limit()
        
        try:
            logger.info(f"Querying Neynar API for channel followers: {channel_id}")
            response = requests.get(base_url, headers=headers, params=params)
            
            response.raise_for_status()
            data = response.json()
            
            return data
        except RequestException as e:
            logger.error(f"Error querying Neynar API for channel followers: {e}")
            return self._handle_request_exception(e)
    
    def get_channel_members(self, channel_id, limit=100):
        """Get members for a specific channel"""
        base_url = "https://api.neynar.com/v2/farcaster/channel/member/list"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "channel_id": channel_id,
            "limit": limit
        }
        
        self.apply_rate_limit()
        
        try:
            logger.info(f"Querying Neynar API for channel members: {channel_id}")
            response = requests.get(base_url, headers=headers, params=params)
            
            response.raise_for_status()
            data = response.json()
            
            return data
        except RequestException as e:
            logger.error(f"Error querying Neynar API for channel members: {e}")
            return self._handle_request_exception(e)
    
    def get_channel_casts(self, channel_id, limit=100, with_replies=True, cutoff_days=None):
        """Get casts for a specific channel"""
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
        
        self.apply_rate_limit()
        
        try:
            logger.info(f"Querying Neynar API for channel casts: {channel_id}")
            response = requests.get(base_url, headers=headers, params=params)
        
            response.raise_for_status()
            data = response.json()
        
            # Skip the timestamp filtering - just return raw data
            return data
        
        except RequestException as e:
            logger.error(f"Error querying Neynar API for channel casts: {e}")
            return self._handle_request_exception(e)
    
    def _handle_request_exception(self, e):
        """Helper to handle request exceptions"""
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status code: {e.response.status_code}")
            logger.error(f"Response content: {e.response.content}")
            
            # Try to parse error response if it's JSON
            try:
                error_data = e.response.json()
                return {"error": error_data}
            except:
                pass
                
        return {"error": str(e)}