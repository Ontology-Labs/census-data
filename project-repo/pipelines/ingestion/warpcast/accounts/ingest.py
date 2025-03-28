from ...helpers import Ingestor
from .cyphers import WarpcastCyphers
import json
import pandas as pd
import logging
import os
import re
from datetime import datetime
import time
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class WarpcastIngester(Ingestor):
    def __init__(self, batch_size=10000, test_mode=False):
        """
        Initialize the WarpcastIngester
        
        Args:
            batch_size (int): Number of records to process in each batch
            test_mode (bool): If True, only process a small subset of data
        """
        self.cyphers = WarpcastCyphers()
        self.asOf = int(datetime.now().timestamp())
        self.batch_size = batch_size
        self.test_mode = test_mode
        self.test_limit = 100  # Number of records to process in test mode
        super().__init__("census-warpcast-account-metadata")  # S3 bucket name

    def load_data(self):
        """
        Load the most recent Warpcast account data file from S3
        """
        logger.info("Loading most recent Warpcast account data from S3")
        
        latest_file = self.get_latest_file('warpcast/users/')
        if not latest_file:
            logger.error("No files found in the S3 bucket")
            return False
            
        content = self.get_file_content(latest_file)
        if not content:
            logger.error("Failed to read file content")
            return False
            
        # Parse the JSON content
        try:
            self.raw_data = json.loads(content)
            logger.info(f"Successfully loaded data with {len(self.raw_data)} batch responses")
            return True
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            return False

    def process_account_data(self):
        """
        Process the raw account data into a format suitable for Neo4j
        """
        logger.info("Processing Warpcast account data")
        
        # Create a list to collect all user data
        all_users = []
        processed_count = 0
        
        # Iterate through each batch in the response
        for batch in self.raw_data:
            if not batch.get('success', False):
                continue
                
            # Extract users data from the batch
            batch_data = batch.get('data', {})
            users = batch_data.get('users', [])
            
            # Add each user to our collection
            for user in users:
                if user:  # Skip empty entries
                    # Flatten the user object for easier CSV handling
                    flat_user = self.flatten_user(user)
                    all_users.append(flat_user)
                    processed_count += 1
                    
                    # Log progress periodically
                    if processed_count % 10000 == 0:
                        logger.info(f"Processed {processed_count} users so far")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_users)
        
        # Log basic statistics
        logger.info(f"Processed {len(df)} users from the raw data")
        
        # If in test mode, limit the number of records
        if self.test_mode:
            logger.info(f"Test mode: limiting to {self.test_limit} records")
            df = df.head(self.test_limit)
        
        self.users_df = df
        return True

    def flatten_user(self, user):
        """
        Flatten the nested user object into a flat dictionary
        """
        flat_user = {
            'fid': user.get('fid'),
            'username': user.get('username'),
            'display_name': user.get('displayName'),
            'custody_address': user.get('custodyAddress'),
            'pfp_url': user.get('pfp', {}).get('url'),
            'bio': user.get('profile', {}).get('bio', {}).get('text'),
            'following_count': user.get('followingCount'),
            'follower_count': user.get('followerCount'),
            'verified': user.get('verified', False),
            'power_badge': user.get('powerBadge', False)
        }
        
        # Extract mentioned profiles from bio if available
        if flat_user['bio'] and isinstance(flat_user['bio'], str):
            # Find all @username mentions in the bio
            mentions = re.findall(r'@(\w+)', flat_user['bio'])
            if mentions:
                flat_user['mentioned_profiles'] = json.dumps(mentions)
        
        # Add additional fields from profile if available
        if 'profile' in user:
            profile = user['profile']
            
            # Add location data if available
            if 'location' in profile and profile['location']:
                location = profile['location']
                
                if 'address' in location and location['address']:
                    address = location['address']
                    flat_user['city'] = address.get('city')
                    flat_user['state'] = address.get('state')
                    flat_user['state_code'] = address.get('stateCode')
                    flat_user['country'] = address.get('country')
                    flat_user['country_code'] = address.get('countryCode')
        
        # Add verification data if available
        if 'verifications' in user and user['verifications']:
            # For simplicity, we'll just take the first verification
            # In a real implementation, you might want to process all verifications
            if len(user['verifications']) > 0:
                verification = user['verifications'][0]
                flat_user['verification_address'] = verification.get('address')
                flat_user['verification_type'] = verification.get('protocol', 'ethereum')
        
        # Clean any null values
        for key, value in flat_user.items():
            if value is None:
                flat_user[key] = ""
                
        return flat_user

    def process_in_batches(self):
        """
        Process the users data in batches, using efficient S3 CSV uploading
        """
        if not hasattr(self, 'users_df'):
            logger.error("No user data available for processing")
            return False
            
        total_users = len(self.users_df)
        logger.info(f"Processing {total_users} users with efficient S3 handling")
        
        # 1. Create accounts in Neo4j
        logger.info("Creating/updating Farcaster accounts")
        accounts_urls = self.save_df_as_csv(self.users_df, f"warpcast_accounts_{self.asOf}")
        
        for i, url in enumerate(accounts_urls):
            logger.info(f"Processing account batch {i+1}/{len(accounts_urls)}")
            count = self.cyphers.create_farcaster_accounts(url)
            logger.info(f"Created/updated {count} Farcaster accounts in Neo4j")
        
        # 2. Link accounts to custody wallets
        custody_df = self.users_df[self.users_df['custody_address'].notna() & (self.users_df['custody_address'] != '')]
        if len(custody_df) > 0:
            logger.info(f"Processing {len(custody_df)} accounts with custody wallets")
            wallet_urls = self.save_df_as_csv(custody_df, f"warpcast_custody_{self.asOf}")
            
            for i, url in enumerate(wallet_urls):
                logger.info(f"Processing wallet batch {i+1}/{len(wallet_urls)}")
                count = self.cyphers.link_accounts_to_wallets(url)
                logger.info(f"Linked {count} accounts to custody wallets")
        
        # 3. Link accounts to verification wallets
        verif_df = self.users_df[self.users_df['verification_address'].notna() & (self.users_df['verification_address'] != '')]
        if len(verif_df) > 0:
            logger.info(f"Processing {len(verif_df)} accounts with verifications")
            verif_urls = self.save_df_as_csv(verif_df, f"warpcast_verif_{self.asOf}")
            
            for i, url in enumerate(verif_urls):
                logger.info(f"Processing verification batch {i+1}/{len(verif_urls)}")
                count = self.cyphers.link_accounts_to_verifications(url)
                logger.info(f"Created {count} verification links")
                
        # 4. Process mentioned profiles
        mentions_df = self.users_df[self.users_df['mentioned_profiles'].notna() & (self.users_df['mentioned_profiles'] != '')]
        if len(mentions_df) > 0:
            logger.info(f"Processing {len(mentions_df)} accounts with bio mentions")
            mentions_urls = self.save_df_as_csv(mentions_df, f"warpcast_mentions_{self.asOf}")
            
            for i, url in enumerate(mentions_urls):
                logger.info(f"Processing mentions batch {i+1}/{len(mentions_urls)}")
                count = self.cyphers.process_mentioned_profiles(url)
                logger.info(f"Processed {count} mention relationships")
        
        return True

    def run(self):
        """
        Run the complete ingestion process
        """
        logger.info("Starting Warpcast account data ingestion")
        start_time = time.time()
        
        # Load data from S3
        if not self.load_data():
            logger.error("Failed to load data, aborting")
            return False
        
        # Process the raw data
        if not self.process_account_data():
            logger.error("Failed to process account data, aborting")
            return False
        
        # Process in batches
        if not self.process_in_batches():
            logger.error("Failed to process batches, aborting")
            return False
        
        elapsed = time.time() - start_time
        logger.info(f"Completed Warpcast account data ingestion in {elapsed:.2f} seconds")
        return True

if __name__ == "__main__":
    # If run directly, use test mode by default
    ingester = WarpcastIngester(test_mode=True)
    ingester.run()