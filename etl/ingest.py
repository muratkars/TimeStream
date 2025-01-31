import requests
import pandas as pd
import os
from minio import Minio

# Config
DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
LOCAL_FILE = "yellow_tripdata_2023-01.parquet"
BUCKET_NAME = "timestream"
OBJECT_NAME = "raw/yellow_tripdata_2023-01.parquet"

# Download dataset
print("Downloading dataset...")
response = requests.get(DATA_URL)
with open(LOCAL_FILE, "wb") as file:
    file.write(response.content)

# Upload to MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

# Ensure bucket exists
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

# Upload file
print("Uploading dataset to MinIO...")
minio_client.fput_object(BUCKET_NAME, OBJECT_NAME, LOCAL_FILE)
print("Upload complete.")