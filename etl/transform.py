"""ETL transformation script for processing TimeStream data from MinIO to Iceberg.

This module handles the transformation of taxi trip data from MinIO storage
to Iceberg tables, including data validation and filtering.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyiceberg.catalog import Catalog, load_catalog

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "TimeStream ETL") -> SparkSession:
    """Initialize and return a Spark session.

    Args:
        app_name: Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_iceberg_catalog(
    uri: str = "http://localhost:19120/api/v1",
    branch: str = "main"
) -> Catalog:
    """Initialize and return the Iceberg catalog.

    Args:
        uri: Nessie API endpoint URI.
        branch: Default branch name.

    Returns:
        Catalog: Configured Iceberg catalog.

    Raises:
        Exception: If catalog initialization fails.
    """
    try:
        return load_catalog("nessie", **{
            "uri": uri,
            "default_branch": branch
        })
    except Exception as e:
        logger.error(f"Failed to load Iceberg catalog: {e}")
        raise

def transform_data(
    spark: SparkSession,
    input_path: str,
    output_table: str,
    min_passengers: int = 0
) -> None:
    """Transform the data and save to Iceberg table.

    Args:
        spark: Active Spark session.
        input_path: Path to input Parquet file.
        output_table: Name of the output Iceberg table.
        min_passengers: Minimum number of passengers for valid trips.

    Raises:
        ValueError: If input parameters are invalid.
        Exception: If transformation or write operations fail.
    """
    if not input_path or not output_table:
        raise ValueError("Input path and output table must be specified")

    try:
        # Read raw data
        df = spark.read.parquet(input_path)
        logger.info(f"Successfully read data from {input_path}")

        # Validate schema
        if "passenger_count" not in df.columns:
            raise ValueError("Input data missing required 'passenger_count' column")

        # Transform: Filter trips
        df_filtered = df.filter(df.passenger_count > min_passengers)
        row_count = df_filtered.count()
        logger.info(f"Filtered data: {row_count} rows")

        if row_count == 0:
            logger.warning("No rows remaining after filtering")

        # Save to Iceberg
        df_filtered.write.format("iceberg").mode("overwrite").save(output_table)
        logger.info(f"Successfully wrote data to {output_table}")

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise

def main() -> None:
    """Main ETL process."""
    try:
        spark = create_spark_session()
        catalog = get_iceberg_catalog()
        
        transform_data(
            spark=spark,
            input_path="s3a://timestream/raw/yellow_tripdata_2023-01.parquet",
            output_table="nessie.timestream.cleaned_trips"
        )
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()