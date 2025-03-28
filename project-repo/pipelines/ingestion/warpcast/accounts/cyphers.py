from ...helpers import Cypher, get_query_logging, count_query_logging
import logging

class WarpcastCyphers(Cypher):
    def __init__(self):
        super().__init__()
        
    @count_query_logging
    def create_farcaster_accounts(self, csv_url):
        """
        Create Farcaster accounts from user data
        """
        logging.info(f"Creating Farcaster accounts from {csv_url}")
        query = f"""
        LOAD CSV WITH HEADERS FROM '{csv_url}' AS row
        MERGE (account:Account:Warpcast {{fid: row.fid}})
        ON CREATE SET
            account.uuid = randomUUID(),
            account.username = row.username,
            account.displayName = row.display_name,
            account.bio = row.bio,
            account.following_count = toInteger(row.following_count),
            account.follower_count = toInteger(row.follower_count),
            account.verified = toBoolean(row.verified),
            account.pfp_url = row.pfp_url,
            account.profile_image = row.profile_image,
            account.power_badge = toBoolean(row.power_badge),
            account.mentioned_profiles = row.mentioned_profiles,
            account.city = row.city,
            account.state = row.state,
            account.state_code = row.state_code,
            account.country = row.country,
            account.country_code = row.country_code,
            account.createdDt = tostring(datetime()),
            account.lastUpdatedDt = tostring(datetime()),
            // Consolidated text field for future embedding
            account.text_content = COALESCE(row.display_name, '') + ' ' + 
                                   COALESCE(row.username, '') + ' ' + 
                                   COALESCE(row.bio, '') + ' ' + 
                                   COALESCE(row.city, '') + ' ' + 
                                   COALESCE(row.state, '') + ' ' + 
                                   COALESCE(row.country, '')
        ON MATCH SET
            account.username = row.username,
            account.displayName = row.display_name,
            account.bio = row.bio,
            account.following_count = toInteger(row.following_count),
            account.follower_count = toInteger(row.follower_count),
            account.verified = toBoolean(row.verified),
            account.pfp_url = row.pfp_url,
            account.profile_image = row.profile_image,
            account.power_badge = toBoolean(row.power_badge),
            account.mentioned_profiles = row.mentioned_profiles,
            account.city = row.city,
            account.state = row.state,
            account.state_code = row.state_code,
            account.country = row.country,
            account.country_code = row.country_code,
            account.lastUpdatedDt = tostring(datetime()),
            // Update consolidated text field
            account.text_content = COALESCE(row.display_name, '') + ' ' + 
                                   COALESCE(row.username, '') + ' ' + 
                                   COALESCE(row.bio, '') + ' ' + 
                                   COALESCE(row.city, '') + ' ' + 
                                   COALESCE(row.state, '') + ' ' + 
                                   COALESCE(row.country, '')
        RETURN count(*)
        """
        return self.query(query)[0]
    
    @count_query_logging
    def link_accounts_to_wallets(self, csv_url):
        """
        Link Farcaster accounts to wallet accounts
        """
        logging.info(f"Linking accounts to wallets from {csv_url}")
        query = f"""
        LOAD CSV WITH HEADERS FROM '{csv_url}' AS row
        WHERE row.custody_address IS NOT NULL AND trim(row.custody_address) <> ''
        
        // Merge the Warpcast account (should already exist)
        MATCH (account:Account:Warpcast {{fid: row.fid}})
        
        // Merge the wallet account - use address as unique identifier
        MERGE (wallet:Account:Wallet {{address: toLower(row.custody_address)}})
        ON CREATE SET
            wallet.uuid = randomUUID(),
            wallet.network = 'ethereum',
            wallet.type = 'custody',
            wallet.createdDt = tostring(datetime()),
            wallet.lastUpdatedDt = tostring(datetime())
        ON MATCH SET
            wallet.lastUpdatedDt = tostring(datetime())
            
        // Create the WALLET relationship
        MERGE (account)-[r:WALLET]->(wallet)
        ON CREATE SET
            r.createdDt = tostring(datetime())
            
        RETURN count(*)
        """
        return self.query(query)[0]
    
    @count_query_logging
    def link_accounts_to_verifications(self, csv_url):
        """
        Link accounts to verification wallets
        """
        logging.info(f"Creating verification links from {csv_url}")
        query = f"""
        LOAD CSV WITH HEADERS FROM '{csv_url}' AS row
        WHERE row.verification_address IS NOT NULL AND trim(row.verification_address) <> ''
        
        // Merge the Warpcast account (should already exist)
        MATCH (account:Account:Warpcast {{fid: row.fid}})
        
        // Merge the wallet account - use address as unique identifier
        MERGE (wallet:Account:Wallet {{address: toLower(row.verification_address)}})
        ON CREATE SET
            wallet.uuid = randomUUID(),
            wallet.network = COALESCE(row.verification_network, 'ethereum'),
            wallet.type = 'verification',
            wallet.createdDt = tostring(datetime()),
            wallet.lastUpdatedDt = tostring(datetime())
        ON MATCH SET
            wallet.lastUpdatedDt = tostring(datetime())
            
        // Create the VERIFY relationship
        MERGE (account)-[r:VERIFY]->(wallet)
        ON CREATE SET
            r.createdDt = tostring(datetime()),
            r.type = COALESCE(row.verification_type, 'address')
            
        RETURN count(*)
        """
        return self.query(query)[0]
        
    @count_query_logging
    def process_mentioned_profiles(self, csv_url):
        """
        Process mentioned profiles in bio text and create MENTIONS relationships
        """
        logging.info(f"Processing mentioned profiles from {csv_url}")
        query = f"""
        LOAD CSV WITH HEADERS FROM '{csv_url}' AS row
        WHERE row.mentioned_profiles IS NOT NULL AND trim(row.mentioned_profiles) <> ''
        
        // Match the account
        MATCH (account:Account:Warpcast {{fid: row.fid}})
        
        // For each mentioned username, try to find the corresponding account
        // and create a MENTIONS relationship
        WITH account, apoc.convert.fromJsonList(row.mentioned_profiles) AS mentions
        UNWIND mentions AS mentioned_username
        
        MATCH (mentioned:Account:Warpcast {{username: mentioned_username}})
        MERGE (account)-[r:MENTIONS]->(mentioned)
        ON CREATE SET
            r.createdDt = tostring(datetime()),
            r.source = 'bio'
        
        RETURN count(*)
        """
        return self.query(query)[0]