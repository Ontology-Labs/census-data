import os
import time
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

class WarpcastAccountScraper:
    def __init__(self, requests_per_minute=5):  # Default to 5 for safety
        self.NEYNAR_API_KEY = os.getenv('NEYNAR_API_KEY')
        self.FARCASTER_EPOCH = datetime(2021, 1, 1, tzinfo=timezone.utc)
        self.last_request_time = time.time()  # Initialize last request time
        
        # Calculate delay between requests based on requests per minute
        # Adding a small buffer (0.2 seconds) for safety
        self.request_delay = (60.0 / requests_per_minute) + 0.2
        logger.info(f"Rate limiting set to {requests_per_minute} requests per minute (delay: {self.request_delay:.2f}s)")

    def convert_timestamp(self, timestamp):
        """Convert Farcaster timestamp to UTC datetime."""
        dt = self.FARCASTER_EPOCH + timedelta(seconds=int(timestamp))
        return dt.timestamp()  # Return Unix timestamp as float
    
    def query_neynar_api_for_users(self, fids):
        base_url = "https://api.neynar.com/v2/farcaster/user/bulk"
        headers = {
            "accept": "application/json",
            "api_key": self.NEYNAR_API_KEY
        }
        params = {
            "fids": ",".join(map(str, fids))  # Convert integers to strings for the API
        }

        # Implement configurable rate limiting
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()  # Update after sleeping

        try:
            logger.info(f"Querying Neynar API for FIDs: {fids[:5]}... (total: {len(fids)})")
            response = requests.get(base_url, headers=headers, params=params)
            
            # Log rate limit information if available in headers
            if 'X-RateLimit-Remaining' in response.headers:
                logger.info(f"Rate limit remaining: {response.headers['X-RateLimit-Remaining']}")
            
            response.raise_for_status()
            data = response.json()
            
            return data
        except RequestException as e:
            logger.error(f"Error querying Neynar API: {e}")
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