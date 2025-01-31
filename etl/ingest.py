"""Data ingestion script for TimeStream project."""

import json
import requests
import pandas as pd
import os
from minio import Minio
from pathlib import Path

# Load config
with open('config/iceberg_config.json') as f:
    config = json.load(f)

# Config
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
LOCAL_FILE = "yellow_tripdata_2023-01.parquet"
BUCKET_NAME = "timestream"
OBJECT_NAME = "raw/yellow_tripdata_2023-01.parquet"

def download_dataset(url: str, local_path: str) -> None:
    """Download dataset from URL."""
    try:
        print(f"Downloading dataset from {url}...")
        response = requests.get(url)
        response.raise_for_status()
        
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as file:
            file.write(response.content)
        print("Download complete.")
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        raise

def upload_to_minio(local_path: str, bucket: str, object_name: str) -> None:
    """Upload file to MinIO."""
    try:
        # Initialize MinIO client
        minio_client = Minio(
            config['s3_endpoint'].replace('http://', ''),
            access_key=config['aws_access_key_id'],
            secret_key=config['aws_secret_access_key'],
            secure=False
        )

        # Ensure bucket exists
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            print(f"Created bucket: {bucket}")

        # Upload file
        print(f"Uploading dataset to MinIO bucket {bucket}...")
        minio_client.fput_object(bucket, object_name, local_path)
        print("Upload complete.")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
        raise

def main():
    """Main execution function."""
    try:
        download_dataset(DATA_URL, LOCAL_FILE)
        upload_to_minio(LOCAL_FILE, BUCKET_NAME, OBJECT_NAME)
    finally:
        # Cleanup
        if os.path.exists(LOCAL_FILE):
            os.remove(LOCAL_FILE)

if __name__ == "__main__":
    main()