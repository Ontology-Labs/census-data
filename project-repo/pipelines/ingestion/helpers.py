import logging
import os
import boto3
import pandas as pd
import json
import time
import math
import re
import sys
import functools
from datetime import datetime
from neo4j import GraphDatabase
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import re

load_dotenv()

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_query_logging(func):
    """Decorator for logging Neo4j query execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Executing query: {func.__name__}")
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.info(f"Query {func.__name__} completed in {elapsed:.2f} seconds")
        return result
    return wrapper

def count_query_logging(func):
    """Decorator for logging Neo4j query execution with count results"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Executing query: {func.__name__}")
        start_time = time.time()
        count = func(*args, **kwargs)
        elapsed = time.time() - start_time
        logger.info(f"Query {func.__name__} affected {count} nodes/relationships in {elapsed:.2f} seconds")
        return count
    return wrapper

class Cypher:
    """
    Base class for Neo4j Cypher queries
    """
    def __init__(self, database=None):
        self.uri = os.getenv("NEO4J_URI")
        self.username = os.getenv("NEO4J_USERNAME")
        self.password = os.getenv("NEO4J_PASSWORD")
        self.database = database
        
        if not self.uri or not self.username or not self.password:
            logger.error("Neo4j connection details not found in environment variables")
            raise ValueError("Neo4j connection details not found")
            
        self.driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
        
    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.close()
    
    def get_drivers(self):
        """Get a list of Neo4j drivers (supports multiple endpoints)"""
        uris = [uri.strip() for uri in os.getenv("NEO4J_URI", "").split(',')]
        usernames = [uri.strip() for uri in os.getenv("NEO4J_USERNAME", "").split(',')]
        passwords = [uri.strip() for uri in os.getenv("NEO4J_PASSWORD", "").split(',')]
        
        assert len(uris) == len(usernames) == len(passwords), "The variables NEO4J_URI, NEO4J_PASSWORD and NEO4J_USERNAME must have the same length"
        
        neo4j_drivers = []
        for uri, username, password in zip(uris, usernames, passwords):
            neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))
            neo4j_drivers.append(neo4j_driver)
        
        return neo4j_drivers
    
    def run_query(self, neo4j_driver, query, parameters=None, counter=0):
        """Run a query using the passed driver. Injects the parameter dict to the query."""
        time.sleep(counter * 10)
        assert neo4j_driver is not None, "Driver not initialized!"
        
        session = None
        response = None
        try:
            session = neo4j_driver.session(database=self.database) if self.database is not None else neo4j_driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            logger.error(f"An error occurred for neo4j instance {neo4j_driver}")
            logger.error(f"Query failed: {e}")
            if counter > 10:
                raise e
            return self.run_query(neo4j_driver, query, parameters=parameters, counter=counter+1)
        finally:
            if session is not None:
                session.close()
        return response
            
    def query(self, query, params=None, last_response_only=True):
        """
        Execute a Cypher query and return the results
        """
        neo4j_drivers = self.get_drivers()
        responses = []
        for neo4j_driver in neo4j_drivers:
            response = self.run_query(neo4j_driver, query, params)
            responses.append(response)
        
        if last_response_only:
            return responses[-1]
        return responses
            
    def sanitize_text(self, text):
        """
        Sanitize text for Neo4j queries
        """
        if not text or not isinstance(text, str):
            return ""
            
        # Replace quotes and escape characters
        sanitized = text.replace('"', "'").replace('\\', '\\\\')
        
        # Remove control characters
        sanitized = re.sub(r'[\x00-\x1F\x7F]', '', sanitized)
        
        return sanitized

class Ingestor:
    """
    Base class for data ingestion pipelines
    """
    def __init__(self, bucket_name):
        """
        Initialize with S3 bucket name
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.s3_resource = boto3.resource(
            's3',
            region_name=os.getenv('AWS_REGION', 'us-east-1'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.bucket = self.get_or_create_bucket()
        
    def get_or_create_bucket(self):
        """Create the S3 bucket if it doesn't exist, or get it if it does"""
        response = self.s3_client.list_buckets()
        
        if self.bucket_name not in [bucket["Name"] for bucket in response["Buckets"]]:
            try:
                logger.warning(f"Bucket not found! Creating {self.bucket_name}")
                location = {"LocationConstraint": os.getenv("AWS_REGION", "us-east-1")}
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration=location
                )
                logger.info(f"Created bucket: {self.bucket_name}")
            except ClientError as e:
                logger.error(f"Error creating bucket {self.bucket_name}: {str(e)}")
                raise e
        else:
            logger.info(f"Using existing bucket: {self.bucket_name}")
            
        return self.s3_resource.Bucket(self.bucket_name)
        
    def get_latest_file(self, prefix):
        """
        Get the latest file from S3 bucket with the given prefix
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                logger.error(f"No files found with prefix {prefix}")
                return None
                
            all_files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            
            if not all_files:
                logger.error(f"No files found with prefix {prefix}")
                return None
                
            latest_file = all_files[0]['Key']
            logger.info(f"Found latest file: {latest_file}")
            
            return latest_file
            
        except Exception as e:
            logger.error(f"Error accessing S3: {str(e)}")
            return None
            
    def get_file_content(self, key):
        """
        Get content of a file from S3
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            return content
            
        except Exception as e:
            logger.error(f"Error getting S3 file content: {str(e)}")
            return None
    
    def check_if_file_exists(self, filename):
        """Check if a file exists in S3"""
        try:
            self.s3_resource.Object(self.bucket_name, filename).load()
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                logger.error(f"Error checking if file exists: {str(e)}")
                raise e
            return False
    
    def save_json(self, filename, data):
        """Save JSON data to S3"""
        try:
            content = json.dumps(data).encode("UTF-8")
            self.s3_client.put_object(Bucket=self.bucket_name, Key=filename, Body=content)
            logger.info(f"Saved JSON to s3://{self.bucket_name}/{filename}")
            return True
        except Exception as e:
            logger.error(f"Error saving JSON to S3: {str(e)}")
            return False
    
    def split_dataframe(self, df, chunk_size=10000):
        """Split a DataFrame into chunks"""
        chunks = []
        num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
        for i in range(num_chunks):
            chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
        return chunks
    
    def save_df_as_csv(self, df, file_name, acl='public-read', max_lines=10000, max_size=10000000):
        """
        Save a DataFrame to CSV files in S3, handling chunking if needed.
        Returns a list of URLs to the CSV files.
        """
        chunks = [df]
        
        # Check if dataframe needs to be split due to size or row count
        if df.memory_usage(index=False).sum() > max_size or len(df) > max_lines:
            chunks = self.split_dataframe(df, chunk_size=max_lines)
        
        logger.info(f"Uploading DataFrame in {len(chunks)} chunks...")
        urls = []
        
        for chunk_id, chunk in enumerate(chunks):
            chunk_name = f"{file_name}--{chunk_id}.csv"
            
            # Save to S3 directly using pandas
            chunk.to_csv(f"s3://{self.bucket_name}/{chunk_name}", index=False, escapechar='\\')
            
            # Set ACL
            self.s3_resource.ObjectAcl(self.bucket_name, chunk_name).put(ACL=acl)
            
            # Get URL
            location = self.s3_client.get_bucket_location(Bucket=self.bucket_name)["LocationConstraint"]
            if location is None:
                location = "us-east-1"  # Default location
            
            url = f"https://s3-{location}.amazonaws.com/{self.bucket_name}/{chunk_name}"
            urls.append(url)
            logger.info(f"Saved chunk {chunk_id+1}/{len(chunks)} to {url}")
        
        return urls