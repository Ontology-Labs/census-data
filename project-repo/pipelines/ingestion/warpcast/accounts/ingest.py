from ...helpers import Ingestor
from .cyphers import WarpcastCyphers
import json
import pandas as pd
import logging
import re
from datetime import datetime
import time
import traceback
from io import StringIO
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class WarpcastIngester(Ingestor):
    def __init__(self, context=None):
        """
        Initialize the WarpcastIngester
        
        Args:
            context: Dagster context for logging
        """
        self.context = context
        self.cyphers = WarpcastCyphers(context=context)
        self.asOf = int(datetime.now().timestamp())
        super().__init__("census-warpcast-account-metadata")  # S3 bucket name
        
    def log(self, message, level="info"):
        """Log using context if available, otherwise use standard logging"""
        if hasattr(self, 'context') and self.context:
            if level == "error":
                self.context.log.error(message)
            else:
                self.context.log.info(message)
        else:
            if level == "error":
                logger.error(message)
            else:
                logger.info(message)

    def load_data(self):
        """
        Load the most recent Warpcast account data file from S3
        """
        self.log("Loading most recent Warpcast account data from S3")
        
        try:
            latest_file = self.get_latest_file('warpcast/users/')
            if not latest_file:
                self.log("No files found in the S3 bucket", "error")
                return False
                
            self.log(f"Found latest file: {latest_file}")
            content = self.get_file_content(latest_file)
            if not content:
                self.log("Failed to read file content", "error")
                return False
                
            # Parse the JSON content
            self.raw_data = json.loads(content)
            self.log(f"Successfully loaded data with {len(self.raw_data)} batch responses")
                
            return True
        except Exception as e:
            self.log(f"Error loading data: {str(e)}", "error")
            self.log(f"Traceback: {traceback.format_exc()}", "error")
            return False

    def process_account_data(self):
        """
        Process the raw account data into a format suitable for Neo4j
        """
        self.log("Processing Warpcast account data")
        
        try:
            # Create a list to collect all user data
            all_users = []
            
            # Iterate through each batch in the response
            for batch_idx, batch in enumerate(self.raw_data):
                if not batch.get('success', False):
                    self.log(f"Skipping unsuccessful batch {batch_idx}", "warning")
                    continue
                    
                # Extract users data from the batch
                batch_data = batch.get('data', {})
                users = batch_data.get('users', [])
                
                self.log(f"Processing batch {batch_idx+1} with {len(users)} users")
                
                # Add each user to our collection
                for user in users:
                    if user:  # Skip empty entries
                        # Flatten the user object for easier CSV handling
                        flat_user = self.flatten_user(user)
                        all_users.append(flat_user)
            
            # Convert to DataFrame
            self.users_df = pd.DataFrame(all_users)
            
            # Log basic statistics
            self.log(f"Processed {len(self.users_df)} users from the raw data")
            
            return True
        except Exception as e:
            self.log(f"Error processing account data: {str(e)}", "error")
            self.log(f"Traceback: {traceback.format_exc()}", "error")
            return False

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
        
        # Handle verifications - they're strings, not objects
        if 'verifications' in user and user['verifications'] and len(user['verifications']) > 0:
            # Just take the first verification address as a string
            flat_user['verification_address'] = user['verifications'][0]
            flat_user['verification_type'] = 'ethereum'  # Default to ethereum
        
        # Clean any null values
        for key, value in flat_user.items():
            if value is None:
                flat_user[key] = ""
                
        return flat_user
        
    def save_df_as_csv(self, df, prefix):
        """
        Save a DataFrame to S3 as CSV and make it publicly accessible
        Returns the S3 URLs of the saved files
        """
        if df.empty:
            self.log("Attempted to save empty DataFrame", "warning")
            return []
            
        self.log(f"Saving DataFrame with {len(df)} rows to S3 with prefix {prefix}")
        
        # Convert to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # Generate filename
        chunk_name = f"{prefix}_{self.asOf}.csv"
        
        try:
            # Upload to S3 with public-read ACL
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=chunk_name,  # No folder path
                Body=csv_data,
                ContentType='text/csv',
                ACL='public-read'  # Make it publicly accessible
            )
            
            # Get the S3 URL
            s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{chunk_name}"
            self.log(f"Successfully saved to {s3_url}")
            
            return [s3_url]
        except Exception as e:
            self.log(f"Error saving to S3: {str(e)}", "error")
            self.log(f"Traceback: {traceback.format_exc()}", "error")
            raise
        
    def process_data(self):
        """
        Process the data into Neo4j
        """
        if not hasattr(self, 'users_df'):
            self.log("No user data available for processing", "error")
            return False
            
        total_users = len(self.users_df)
        self.log(f"Processing {total_users} Warpcast users to Neo4j")
        
        try:
            # 1. Create accounts in Neo4j
            self.log("Creating/updating Farcaster accounts")
            accounts_url = self.save_df_as_csv(self.users_df, f"warpcast_accounts_{self.asOf}")[0]
            count = self.cyphers.create_farcaster_accounts(accounts_url)
            self.log(f"Created/updated {count} Farcaster accounts in Neo4j")
            
            # 2. Link accounts to custody wallets
            custody_df = self.users_df[self.users_df['custody_address'].notna() & (self.users_df['custody_address'] != '')]
            if len(custody_df) > 0:
                self.log(f"Processing {len(custody_df)} accounts with custody wallets")
                wallet_url = self.save_df_as_csv(custody_df, f"warpcast_custody_{self.asOf}")[0]
                count = self.cyphers.link_accounts_to_wallets(wallet_url)
                self.log(f"Linked {count} accounts to custody wallets")
            else:
                self.log("No accounts with custody wallets found")
            
            # 3. Link accounts to verification wallets
            verif_df = self.users_df[self.users_df['verification_address'].notna() & (self.users_df['verification_address'] != '')]
            if len(verif_df) > 0:
                self.log(f"Processing {len(verif_df)} accounts with verifications")
                verif_url = self.save_df_as_csv(verif_df, f"warpcast_verif_{self.asOf}")[0]
                count = self.cyphers.link_accounts_to_verifications(verif_url)
                self.log(f"Created {count} verification links")
            else:
                self.log("No accounts with verification addresses found")
                    
            # 4. Process mentioned profiles
            mentions_df = self.users_df[self.users_df['mentioned_profiles'].notna() & (self.users_df['mentioned_profiles'] != '')]
            if len(mentions_df) > 0:
                self.log(f"Processing {len(mentions_df)} accounts with bio mentions")
                mentions_url = self.save_df_as_csv(mentions_df, f"warpcast_mentions_{self.asOf}")[0]
                count = self.cyphers.process_mentioned_profiles(mentions_url)
                self.log(f"Processed {count} mention relationships")
            else:
                self.log("No accounts with bio mentions found")
            
            return True
            
        except Exception as e:
            self.log(f"Error in data processing: {str(e)}", "error")
            self.log(f"Traceback: {traceback.format_exc()}", "error")
            return False

    def run(self):
        """
        Run the complete ingestion process
        """
        self.log("Starting Warpcast account data ingestion")
        start_time = time.time()
        
        try:
            # Load data from S3
            if not self.load_data():
                self.log("Failed to load data, aborting", "error")
                return False
            
            # Process the raw data
            if not self.process_account_data():
                self.log("Failed to process account data, aborting", "error")
                return False
            
            # Process data to Neo4j
            if not self.process_data():
                self.log("Failed to process data to Neo4j, aborting", "error")
                return False
            
            elapsed = time.time() - start_time
            self.log(f"Completed Warpcast account ingestion in {elapsed:.2f} seconds")
            return True
            
        except Exception as e:
            self.log(f"Unhandled exception in ingestion: {str(e)}", "error")
            self.log(f"Traceback: {traceback.format_exc()}", "error")
            return False