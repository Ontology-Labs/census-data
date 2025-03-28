# pipelines/scraping/warpcast/channels/scrape.py

import os
import time
import json
import requests as r
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

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

    def get_channel_metadata(self, channel_id):
        """
        Fetch channel metadata by channel ID (or 'channel_name') from the Neynar API.
        """
        self.apply_rate_limit()  # apply rate limit before calling the API

        base_url = "https://api.neynar.com/v2/farcaster/channel"
        headers = {
            'accept': 'application/json',
            'api_key': self.NEYNAR_API_KEY
        }
        params = {
            'id': channel_id
        }

        try:
            logger.info(f"API Request: GET {base_url} - ID: {channel_id}")
            response = r.get(base_url, headers=headers, params=params)
            logger.info(f"API Response: Status {response.status_code} - ID: {channel_id}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Metadata Response: Channel keys present: {', '.join(data.keys())}")
            return data
        except r.RequestException as e:
            logger.error(f"Error fetching channel metadata: {e}")
            return self._handle_request_exception(e)

    def get_channel_followers(self, channel_id, limit=1000):
        """
        Get followers for a specific channel from the Neynar API.
        Handles pagination to get all followers.
        """
        self.apply_rate_limit()
        
        base_url = "https://api.neynar.com/v2/farcaster/channel/followers"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        
        all_followers = []
        cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            params = {
                "id": channel_id,
                "limit": min(limit, 100)  # API limit is 100 per request
            }
            
            if cursor:
                params["cursor"] = cursor
            
            try:
                logger.info(f"Followers API Request (Page {page_count}): GET {base_url} - ID: {channel_id}, Cursor: {cursor}")
                response = r.get(base_url, headers=headers, params=params)
                logger.info(f"Followers API Response (Page {page_count}): Status {response.status_code} - ID: {channel_id}")
                response.raise_for_status()
                data = response.json()
                logger.info(f"Followers Response Keys: {', '.join(data.keys())}")
                
                # Add followers from this page
                if "followers" in data:
                    batch_followers = data["followers"]
                    all_followers.extend(batch_followers)
                    logger.info(f"Retrieved {len(batch_followers)} followers on page {page_count} (total: {len(all_followers)})")
                else:
                    logger.warning(f"No 'followers' key in response for page {page_count}")
                
                # Check if there are more followers to fetch
                if "next" in data and data["next"] and "cursor" in data["next"]:
                    cursor = data["next"]["cursor"]
                    logger.info(f"Next cursor found: {cursor}")
                    
                    # If we've reached the limit, stop
                    if len(all_followers) >= limit:
                        logger.info(f"Reached follower limit ({limit}), stopping pagination after page {page_count}")
                        break
                else:
                    logger.info(f"No more pages available, stopping pagination after page {page_count}")
                    # No more pages
                    break
                
                # Apply rate limiting
                self.apply_rate_limit()
                
            except r.RequestException as e:
                logger.error(f"Error querying Neynar API for channel followers: {e}")
                return self._handle_request_exception(e)
        
        logger.info(f"Completed followers fetch: {len(all_followers)} total followers from {page_count} pages")
        return {"followers": all_followers}

    def get_channel_members(self, channel_id, limit=1000):
        """
        Get members for a specific channel from the Neynar API.
        Handles pagination to get all members.
        """
        self.apply_rate_limit()

        base_url = "https://api.neynar.com/v2/farcaster/channel/member/list"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        
        all_members = []
        cursor = None
        page_count = 0
        
        while True:
            page_count += 1
            params = {
                "channel_id": channel_id,
                "limit": min(limit, 100)  # API limit is 100 per request
            }
            
            if cursor:
                params["cursor"] = cursor
            
            try:
                logger.info(f"Members API Request (Page {page_count}): GET {base_url} - ID: {channel_id}, Cursor: {cursor}")
                response = r.get(base_url, headers=headers, params=params)
                logger.info(f"Members API Response (Page {page_count}): Status {response.status_code} - ID: {channel_id}")
                response.raise_for_status()
                data = response.json()
                logger.info(f"Members Response Keys: {', '.join(data.keys())}")
                
                # Add members from this page
                if "members" in data:
                    batch_members = data["members"]
                    all_members.extend(batch_members)
                    logger.info(f"Retrieved {len(batch_members)} members on page {page_count} (total: {len(all_members)})")
                else:
                    logger.warning(f"No 'members' key in response for page {page_count}")
                
                # Check if there are more members to fetch
                if "next" in data and data["next"] and "cursor" in data["next"]:
                    cursor = data["next"]["cursor"]
                    logger.info(f"Next cursor found: {cursor}")
                    
                    # If we've reached the limit, stop
                    if len(all_members) >= limit:
                        logger.info(f"Reached member limit ({limit}), stopping pagination after page {page_count}")
                        break
                else:
                    logger.info(f"No more pages available, stopping pagination after page {page_count}")
                    # No more pages
                    break
                
                # Apply rate limiting
                self.apply_rate_limit()
                
            except r.RequestException as e:
                logger.error(f"Error querying Neynar API for channel members: {e}")
                return self._handle_request_exception(e)
        
        logger.info(f"Completed members fetch: {len(all_members)} total members from {page_count} pages")
        return {"members": all_members}

    def get_channel_casts(self, channel_id, limit=100, with_replies=True, cutoff_days=None):
        """
        Get casts for a specific channel from the Neynar API, optionally filtering
        out older casts via 'cutoff_days'. Handles pagination.
        """
        self.apply_rate_limit()

        base_url = "https://api.neynar.com/v2/farcaster/feed/channels"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        
        all_casts = []
        cursor = None
        reached_cutoff = False
        page_count = 0
        
        # Calculate cutoff datetime if needed
        cutoff_dt = None
        if cutoff_days:
            cutoff_dt = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
            logger.info(f"Using cutoff date: {cutoff_dt.isoformat()}")
        
        while not reached_cutoff:
            page_count += 1
            params = {
                "channel_ids": channel_id,
                "limit": min(limit, 100),  # API limit is 100 per request
                "with_replies": with_replies
            }
            
            if cursor:
                params["cursor"] = cursor
            
            try:
                logger.info(f"Casts API Request (Page {page_count}): GET {base_url} - ID: {channel_id}, Cursor: {cursor}")
                response = r.get(base_url, headers=headers, params=params)
                logger.info(f"Casts API Response (Page {page_count}): Status {response.status_code} - ID: {channel_id}")
                response.raise_for_status()
                data = response.json()
                logger.info(f"Casts Response Keys: {', '.join(data.keys())}")
                
                # Add casts from this page, filtering by cutoff if needed
                if "casts" in data:
                    batch_casts = data["casts"]
                    logger.info(f"Retrieved {len(batch_casts)} casts on page {page_count}")
                    
                    if cutoff_dt:
                        filtered_batch = []
                        for cast in batch_casts:
                            cast_dt = self._parse_cast_timestamp(cast["timestamp"])
                            if cast_dt >= cutoff_dt:
                                filtered_batch.append(cast)
                            else:
                                # We've reached casts older than our cutoff
                                reached_cutoff = True
                        
                        all_casts.extend(filtered_batch)
                        logger.info(f"Added {len(filtered_batch)} casts after filtering by date (total: {len(all_casts)})")
                        
                        # If this batch had casts filtered out due to age, we've reached the cutoff
                        if len(filtered_batch) < len(batch_casts):
                            logger.info(f"Reached cutoff date in page {page_count}, stopping pagination")
                            break
                    else:
                        all_casts.extend(batch_casts)
                        logger.info(f"Added {len(batch_casts)} casts (total: {len(all_casts)})")
                else:
                    logger.warning(f"No 'casts' key in response for page {page_count}")
                
                # Check if there are more casts to fetch
                if "next" in data and data["next"] and "cursor" in data["next"]:
                    cursor = data["next"]["cursor"]
                    logger.info(f"Next cursor found: {cursor}")
                    
                    # If we've reached the limit, stop
                    if len(all_casts) >= limit:
                        logger.info(f"Reached cast limit ({limit}), stopping pagination after page {page_count}")
                        break
                else:
                    logger.info(f"No more pages available, stopping pagination after page {page_count}")
                    # No more pages
                    break
                
                # Apply rate limiting
                self.apply_rate_limit()
                
            except r.RequestException as e:
                logger.error(f"Error querying Neynar API for channel casts: {e}")
                return self._handle_request_exception(e)
        
        logger.info(f"Completed casts fetch: {len(all_casts)} total casts from {page_count} pages")
        return {"casts": all_casts}