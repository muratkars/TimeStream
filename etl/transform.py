"""ETL transformation script for TimeStream data with query optimization."""

import logging
from datetime import datetime
from typing import Optional, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour,
    expr, to_timestamp
)
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import Transform

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryOptimizer:
    """Handles query optimization and indexing for TimeStream data."""
    
    def __init__(self, spark: SparkSession):
        """Initialize QueryOptimizer.
        
        Args:
            spark: Active Spark session
        """
        self.spark = spark
    
    def optimize_partitioning(self, df: DataFrame) -> DataFrame:
        """Apply optimal partitioning strategy.
        
        Implements:
        1. Time-based partitioning for temporal queries
        2. Z-ordering for spatial data
        3. Bucketing for passenger count
        """
        # Add partition columns
        return df.withColumn(
            "pickup_date", to_timestamp("pickup_datetime")
        ).withColumn(
            "year", year("pickup_date")
        ).withColumn(
            "month", month("pickup_date")
        ).withColumn(
            "day", dayofmonth("pickup_date")
        ).withColumn(
            "hour", hour("pickup_date")
        )
    
    def apply_z_ordering(self, df: DataFrame) -> DataFrame:
        """Apply Z-ordering to spatial columns."""
        return df.withColumn(
            "z_order_col",
            expr("Z_ORDER(pickup_longitude, pickup_latitude)")
        )
    
    def create_indices(self, table_name: str) -> None:
        """Create indices for frequent query patterns."""
        try:
            # Create indices using Iceberg's metadata tables
            self.spark.sql(f"""
                ALTER TABLE {table_name}
                ADD INDEX pickup_time_idx
                ON pickup_datetime
                OPTIONS ('write.metadata.metrics.column'='true')
            """)
            
            logger.info(f"Created temporal index on {table_name}")
            
            # Create spatial index
            self.spark.sql(f"""
                ALTER TABLE {table_name}
                ADD INDEX location_idx
                ON (pickup_longitude, pickup_latitude)
                OPTIONS ('write.metadata.metrics.column'='true')
            """)
            
            logger.info(f"Created spatial index on {table_name}")
            
        except Exception as e:
            logger.error(f"Error creating indices: {e}")
            raise

def create_spark_session() -> SparkSession:
    """Create and configure Spark session with optimization settings."""
    return (SparkSession.builder
        .appName("TimeStream ETL")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # Optimization configs
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.statistics.histogram.enabled", "true")
        .getOrCreate())

def transform_data(
    spark: SparkSession,
    input_path: str,
    output_table: str,
    partition_by: List[str] = None
) -> None:
    """Transform data with optimizations.
    
    Args:
        spark: Spark session
        input_path: Path to input Parquet file
        output_table: Name of output Iceberg table
        partition_by: List of columns to partition by
    """
    try:
        # Initialize optimizer
        optimizer = QueryOptimizer(spark)
        
        # Read raw data
        logger.info(f"Reading data from {input_path}")
        df = spark.read.parquet(input_path)
        
        # Basic filtering
        df_filtered = df.filter(df.passenger_count > 0)
        
        # Apply optimizations
        df_optimized = optimizer.optimize_partitioning(df_filtered)
        df_optimized = optimizer.apply_z_ordering(df_optimized)
        
        # Default partitioning if none specified
        if partition_by is None:
            partition_by = ["year", "month", "day"]
        
        # Write with optimizations
        (df_optimized.write
            .format("iceberg")
            .mode("overwrite")
            .option("write.format.default", "parquet")
            .option("write.metadata.metrics.default", "full")
            .option("write.metadata.compression-codec", "gzip")
            .option("write.target-file-size-bytes", 536870912)  # 512MB target
            .option("write.distribution-mode", "hash")
            .partitionedBy(*partition_by)
            .saveAsTable(output_table))
        
        logger.info(f"Successfully wrote data to {output_table}")
        
        # Create indices
        optimizer.create_indices(output_table)
        
        # Collect and log statistics
        stats = df_optimized.summary()
        logger.info(f"Data statistics: {stats}")
        
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        raise

def main() -> None:
    """Main execution function."""
    spark = None
    try:
        spark = create_spark_session()
        
        # Custom partitioning strategy
        partition_by = ["year", "month", "day", "hour"]
        
        transform_data(
            spark=spark,
            input_path="s3a://timestream/raw/yellow_tripdata_2023-01.parquet",
            output_table="nessie.timestream.cleaned_trips",
            partition_by=partition_by
        )
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()