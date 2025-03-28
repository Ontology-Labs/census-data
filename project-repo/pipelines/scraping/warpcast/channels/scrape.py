# pipelines/scraping/warpcast/channels/scrape.py

import os
import time
import json
import requests as r
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import traceback

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
        logger.info(f"Rate limiting set to {requests_per_minute} requests/minute (delay: {self.request_delay:.2f}s)")

    def apply_rate_limit(self):
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def _parse_cast_timestamp(self, timestamp_str):
        try:
            # Try interpreting timestamp_str as an integer offset from FARCASTER_EPOCH
            try:
                offset_seconds = int(timestamp_str)
                return self.FARCASTER_EPOCH + timedelta(seconds=offset_seconds)
            except (TypeError, ValueError):
                # Fall back to parsing as an ISO8601 date string
                if timestamp_str and isinstance(timestamp_str, str) and timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            logger.error(f"Error parsing timestamp '{timestamp_str}': {e}")
            # Return current time as fallback
            return datetime.now(timezone.utc)

    def get_channel_metadata(self, channel_id):
        try:
            self.apply_rate_limit()
            
            base_url = "https://api.neynar.com/v2/farcaster/channel"
            headers = {
                'accept': 'application/json',
                'api_key': self.NEYNAR_API_KEY
            }
            params = {
                'id': channel_id
            }

            logger.info(f"Fetching channel metadata for: {channel_id}")
            
            try:
                response = r.get(base_url, headers=headers, params=params, timeout=30)
                logger.info(f"Metadata response received with status {response.status_code}")
                
                # Even if we get an error status, try to return what we have
                if response.status_code != 200:
                    logger.warning(f"Non-200 status code: {response.status_code} for metadata")
                    try:
                        return response.json()  # Try to parse response anyway
                    except:
                        return {"error": f"Status code: {response.status_code}"}
                
                data = response.json()
                logger.info(f"Metadata successfully parsed")
                return data
            except Exception as e:
                logger.error(f"Error in metadata request: {str(e)}")
                return {"error": str(e)}
                
        except Exception as e:
            logger.error(f"Unexpected error in get_channel_metadata: {e}")
            return {"error": f"Unexpected error: {str(e)}"}

    def get_channel_followers(self, channel_id, limit=1000):
        try:
            self.apply_rate_limit()
            
            base_url = "https://api.neynar.com/v2/farcaster/channel/followers"
            headers = {
                "accept": "application/json",
                "api_key": self.NEYNAR_API_KEY
            }
            
            all_followers = []
            cursor = None
            page_count = 0
            
            # Maximum number of pages to try (safety limit)
            max_pages = 100
            
            while page_count < max_pages:
                try:
                    page_count += 1
                    params = {
                        "id": channel_id,
                        "limit": min(limit, 100)  # API limit is 100 per request
                    }
                    
                    # Only add cursor if it exists and is not None or empty
                    if cursor:
                        params["cursor"] = cursor
                    
                    logger.info(f"Followers request - page {page_count} for channel {channel_id}")
                    
                    response = r.get(base_url, headers=headers, params=params, timeout=30)
                    logger.info(f"Followers response received with status {response.status_code}")
                    
                    # Even if we get an error status, try to return what we have
                    if response.status_code != 200:
                        logger.warning(f"Non-200 status code: {response.status_code} for followers page {page_count}")
                        break  # Stop pagination on error
                    
                    data = response.json()
                    
                    # Add followers from this page if available
                    if data and "followers" in data:
                        batch_followers = data["followers"]
                        all_followers.extend(batch_followers)
                        logger.info(f"Retrieved {len(batch_followers)} followers on page {page_count} (total: {len(all_followers)})")
                    else:
                        logger.warning(f"No 'followers' key in response for page {page_count}")
                    
                    # Handle pagination safely
                    next_cursor = None
                    try:
                        if data and "next" in data and data["next"] and "cursor" in data["next"]:
                            next_cursor = data["next"]["cursor"]
                            if next_cursor:
                                # Just check if cursor exists but don't try to access it
                                logger.info(f"Next cursor found for page {page_count+1}")
                    except Exception as e:
                        logger.error(f"Error extracting cursor: {e}")
                        next_cursor = None
                    
                    # Update cursor for next iteration
                    cursor = next_cursor
                    
                    # If there's no cursor or we've reached the limit, stop
                    if not cursor:
                        logger.info(f"No more pages available, stopping pagination after page {page_count}")
                        break
                        
                    if len(all_followers) >= limit:
                        logger.info(f"Reached follower limit ({limit}), stopping pagination after page {page_count}")
                        break
                    
                    # Apply rate limiting
                    self.apply_rate_limit()
                    
                except Exception as e:
                    logger.error(f"Error on followers page {page_count}: {str(e)}")
                    logger.error(f"Will return the {len(all_followers)} followers collected so far")
                    break  # Stop on error but return what we have
                
            # Always return what we have, even if we hit an error
            logger.info(f"Completed followers fetch: {len(all_followers)} total followers from {page_count} pages")
            return {"followers": all_followers}
                
        except Exception as e:
            logger.error(f"Unexpected error in get_channel_followers: {e}")
            # Return empty list if we fail completely
            return {"followers": []}

    def get_channel_members(self, channel_id, limit=1000):
        try:
            self.apply_rate_limit()

            base_url = "https://api.neynar.com/v2/farcaster/channel/member/list"
            headers = {
                "accept": "application/json",
                "api_key": self.NEYNAR_API_KEY
            }
            
            all_members = []
            cursor = None
            page_count = 0
            
            # Maximum number of pages to try (safety limit)
            max_pages = 100
            
            while page_count < max_pages:
                try:
                    page_count += 1
                    params = {
                        "channel_id": channel_id,
                        "limit": min(limit, 100)  # API limit is 100 per request
                    }
                    
                    # Only add cursor if it exists and is not None or empty
                    if cursor:
                        params["cursor"] = cursor
                    
                    logger.info(f"Members request - page {page_count} for channel {channel_id}")
                    
                    response = r.get(base_url, headers=headers, params=params, timeout=30)
                    logger.info(f"Members response received with status {response.status_code}")
                    
                    # Even if we get an error status, try to return what we have
                    if response.status_code != 200:
                        logger.warning(f"Non-200 status code: {response.status_code} for members page {page_count}")
                        break  # Stop pagination on error
                    
                    data = response.json()
                    
                    # Add members from this page if available
                    if data and "members" in data:
                        batch_members = data["members"]
                        all_members.extend(batch_members)
                        logger.info(f"Retrieved {len(batch_members)} members on page {page_count} (total: {len(all_members)})")
                    else:
                        logger.warning(f"No 'members' key in response for page {page_count}")
                    
                    # Handle pagination safely
                    next_cursor = None
                    try:
                        if data and "next" in data and data["next"] and "cursor" in data["next"]:
                            next_cursor = data["next"]["cursor"]
                            if next_cursor:
                                # Just check if cursor exists but don't try to access it
                                logger.info(f"Next cursor found for page {page_count+1}")
                    except Exception as e:
                        logger.error(f"Error extracting cursor: {e}")
                        next_cursor = None
                    
                    # Update cursor for next iteration
                    cursor = next_cursor
                    
                    # If there's no cursor or we've reached the limit, stop
                    if not cursor:
                        logger.info(f"No more pages available, stopping pagination after page {page_count}")
                        break
                        
                    if len(all_members) >= limit:
                        logger.info(f"Reached member limit ({limit}), stopping pagination after page {page_count}")
                        break
                    
                    # Apply rate limiting
                    self.apply_rate_limit()
                    
                except Exception as e:
                    logger.error(f"Error on members page {page_count}: {str(e)}")
                    logger.error(f"Will return the {len(all_members)} members collected so far")
                    break  # Stop on error but return what we have
            
            # Always return what we have, even if we hit an error
            logger.info(f"Completed members fetch: {len(all_members)} total members from {page_count} pages")
            return {"members": all_members}
                
        except Exception as e:
            logger.error(f"Unexpected error in get_channel_members: {e}")
            # Return empty list if we fail completely
            return {"members": []}

    def get_channel_casts(self, channel_id, limit=100, with_replies=True, cutoff_days=None):
        try:
            self.apply_rate_limit()

            base_url = "https://api.neynar.com/v2/farcaster/feed/channels"
            headers = {
                "accept": "application/json",
                "api_key": self.NEYNAR_API_KEY
            }
            
            all_casts = []
            cursor = None
            page_count = 0
            reached_cutoff = False
            
            # Calculate cutoff datetime if needed
            cutoff_dt = None
            if cutoff_days:
                try:
                    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
                    logger.info(f"Using cutoff date: {cutoff_dt.isoformat()}")
                except Exception as e:
                    logger.error(f"Error calculating cutoff date: {e}")
                    cutoff_dt = None
            
            # Maximum number of pages to try (safety limit)
            max_pages = 100
            
            while page_count < max_pages and not reached_cutoff:
                try:
                    page_count += 1
                    params = {
                        "channel_ids": channel_id,
                        "limit": min(limit, 100),  # API limit is 100 per request
                        "with_replies": with_replies
                    }
                    
                    # Only add cursor if it exists and is not None or empty
                    if cursor:
                        params["cursor"] = cursor
                    
                    logger.info(f"Casts request - page {page_count} for channel {channel_id}")
                    
                    response = r.get(base_url, headers=headers, params=params, timeout=30)
                    logger.info(f"Casts response received with status {response.status_code}")
                    
                    # Even if we get an error status, try to return what we have
                    if response.status_code != 200:
                        logger.warning(f"Non-200 status code: {response.status_code} for casts page {page_count}")
                        break  # Stop pagination on error
                    
                    data = response.json()
                    
                    # Add casts from this page, filtering by cutoff if needed
                    if data and "casts" in data:
                        batch_casts = data["casts"]
                        logger.info(f"Retrieved {len(batch_casts)} casts on page {page_count}")
                        
                        if cutoff_dt:
                            try:
                                filtered_batch = []
                                for cast in batch_casts:
                                    try:
                                        cast_dt = self._parse_cast_timestamp(cast.get("timestamp"))
                                        if cast_dt >= cutoff_dt:
                                            filtered_batch.append(cast)
                                        else:
                                            # We've reached casts older than our cutoff
                                            reached_cutoff = True
                                    except Exception as e:
                                        logger.error(f"Error processing cast: {e}")
                                        # Include the cast anyway if we can't parse the timestamp
                                        filtered_batch.append(cast)
                                
                                all_casts.extend(filtered_batch)
                                logger.info(f"Added {len(filtered_batch)} casts after filtering by date (total: {len(all_casts)})")
                                
                                # If this batch had casts filtered out due to age, we've reached the cutoff
                                if len(filtered_batch) < len(batch_casts):
                                    logger.info(f"Reached cutoff date in page {page_count}, stopping pagination")
                                    break
                            except Exception as e:
                                logger.error(f"Error during date filtering: {e}")
                                # Fall back to adding all casts
                                all_casts.extend(batch_casts)
                                logger.info(f"Added all {len(batch_casts)} casts without filtering (total: {len(all_casts)})")
                        else:
                            all_casts.extend(batch_casts)
                            logger.info(f"Added {len(batch_casts)} casts (total: {len(all_casts)})")
                    else:
                        logger.warning(f"No 'casts' key in response for page {page_count}")
                    
                    # Handle pagination safely
                    next_cursor = None
                    try:
                        if data and "next" in data and data["next"] and "cursor" in data["next"]:
                            next_cursor = data["next"]["cursor"]
                            if next_cursor:
                                # Just check if cursor exists but don't try to access it
                                logger.info(f"Next cursor found for page {page_count+1}")
                    except Exception as e:
                        logger.error(f"Error extracting cursor: {e}")
                        next_cursor = None
                    
                    # Update cursor for next iteration
                    cursor = next_cursor
                    
                    # If there's no cursor or we've reached the limit, stop
                    if not cursor:
                        logger.info(f"No more pages available, stopping pagination after page {page_count}")
                        break
                        
                    if len(all_casts) >= limit:
                        logger.info(f"Reached cast limit ({limit}), stopping pagination after page {page_count}")
                        break
                    
                    # Apply rate limiting
                    self.apply_rate_limit()
                    
                except Exception as e:
                    logger.error(f"Error on casts page {page_count}: {str(e)}")
                    logger.error(f"Will return the {len(all_casts)} casts collected so far")
                    break  # Stop on error but return what we have
            
            # Always return what we have, even if we hit an error
            logger.info(f"Completed casts fetch: {len(all_casts)} total casts from {page_count} pages")
            return {"casts": all_casts}
                
        except Exception as e:
            logger.error(f"Unexpected error in get_channel_casts: {e}")
            # Return empty list if we fail completely
            return {"casts": []}