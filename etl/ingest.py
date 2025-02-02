"""Data ingestion script for TimeStream project with optimized data transfer."""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from minio import Minio
from minio.commonconfig import ENABLED, Filter
from minio.lifecycleconfig import LifecycleConfig, Rule, Transition
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataIngestionManager:
    """Manages optimized data ingestion with parallel processing and monitoring."""
    
    def __init__(
        self,
        config_path: str = 'config/iceberg_config.json',
        chunk_size: int = 1024 * 1024 * 8,  # 8MB chunks
        max_workers: int = 4
    ):
        """Initialize ingestion manager.
        
        Args:
            config_path: Path to configuration file
            chunk_size: Size of upload chunks in bytes
            max_workers: Maximum number of concurrent workers
        """
        self.config = self._load_config(config_path)
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.minio_client = self._init_minio_client()
        self.session = self._init_requests_session()
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
            
    def _init_minio_client(self) -> Minio:
        """Initialize MinIO client with retry logic."""
        try:
            return Minio(
                self.config['s3_endpoint'].replace('http://', ''),
                access_key=self.config['aws_access_key_id'],
                secret_key=self.config['aws_secret_access_key'],
                secure=False
            )
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise
            
    def _init_requests_session(self) -> requests.Session:
        """Initialize requests session with retry logic."""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504]
        )
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session
        
    def _setup_bucket(self, bucket_name: str) -> None:
        """Setup bucket with lifecycle policies."""
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
                
            # Configure lifecycle policy
            config = LifecycleConfig(
                [
                    Rule(
                        ENABLED,
                        rule_filter=Filter(prefix="raw/"),
                        rule_id="raw_data_cleanup",
                        transition=Transition(days=30, storage_class="GLACIER")
                    )
                ]
            )
            self.minio_client.set_bucket_lifecycle(bucket_name, config)
            logger.info(f"Configured lifecycle policy for {bucket_name}")
            
        except Exception as e:
            logger.error(f"Bucket setup failed: {e}")
            raise
            
    def download_file(self, url: str, local_path: str) -> None:
        """Download file with progress bar and chunked transfer."""
        try:
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(local_path, 'wb') as f, tqdm(
                desc=f"Downloading {Path(local_path).name}",
                total=total_size,
                unit='iB',
                unit_scale=True
            ) as pbar:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    size = f.write(chunk)
                    pbar.update(size)
                    
        except Exception as e:
            logger.error(f"Download failed for {url}: {e}")
            raise
            
    def upload_file(
        self,
        local_path: str,
        bucket: str,
        object_name: str,
        metadata: Optional[Dict] = None
    ) -> None:
        """Upload file with progress monitoring."""
        try:
            file_size = os.path.getsize(local_path)
            
            with tqdm(
                desc=f"Uploading {Path(local_path).name}",
                total=file_size,
                unit='iB',
                unit_scale=True
            ) as pbar:
                self.minio_client.fput_object(
                    bucket,
                    object_name,
                    local_path,
                    metadata=metadata or {}
                )
                pbar.update(file_size)
                
        except Exception as e:
            logger.error(f"Upload failed for {local_path}: {e}")
            raise
            
    def process_dataset(
        self,
        url: str,
        bucket: str,
        object_prefix: str
    ) -> None:
        """Process a single dataset."""
        try:
            # Generate paths
            filename = Path(urlparse(url).path).name
            local_path = f"temp/{filename}"
            object_name = f"{object_prefix}/{filename}"
            
            # Download
            self.download_file(url, local_path)
            
            # Prepare metadata
            metadata = {
                "source_url": url,
                "ingestion_time": datetime.now().isoformat(),
                "content_type": "application/parquet"
            }
            
            # Upload
            self.upload_file(local_path, bucket, object_name, metadata)
            
        finally:
            # Cleanup
            if os.path.exists(local_path):
                os.remove(local_path)
                
    def ingest_datasets(
        self,
        urls: List[str],
        bucket: str,
        object_prefix: str = "raw"
    ) -> None:
        """Ingest multiple datasets in parallel."""
        try:
            # Setup bucket
            self._setup_bucket(bucket)
            
            # Create temp directory
            Path("temp").mkdir(exist_ok=True)
            
            # Process datasets in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self.process_dataset,
                        url,
                        bucket,
                        object_prefix
                    )
                    for url in urls
                ]
                
                for future in as_completed(futures):
                    future.result()  # Will raise any exceptions that occurred
                    
        finally:
            # Cleanup temp directory
            if os.path.exists("temp"):
                os.rmdir("temp")

def main():
    """Main execution function."""
    try:
        # Initialize manager
        manager = DataIngestionManager()
        
        # Dataset URLs
        urls = [
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
        ]
        
        # Process datasets
        manager.ingest_datasets(
            urls=urls,
            bucket="timestream",
            object_prefix="raw"
        )
        
        logger.info("Data ingestion completed successfully")
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise

if __name__ == "__main__":
    main()