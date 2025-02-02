"""Data format conversion and handling module for TimeStream project."""

import logging
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import delta
from delta.tables import DeltaTable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFormat(Enum):
    """Supported data formats."""
    PARQUET = "parquet"
    DELTA = "delta"
    CSV = "csv"
    JSON = "json"
    ORC = "orc"
    AVRO = "avro"

class DataConverter:
    """Handles data format conversion and schema management."""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """Initialize DataConverter.
        
        Args:
            spark: Optional SparkSession. If not provided, creates new one.
        """
        self.spark = spark or self._create_spark_session()
        
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with necessary configurations."""
        return (SparkSession.builder
            .appName("TimeStream Data Converter")
            .config("spark.jars.packages", 
                   "io.delta:delta-core_2.12:2.4.0,"  # Delta Lake
                   "org.apache.spark:spark-avro_2.12:3.5.0")  # Avro support
            .config("spark.sql.extensions", 
                   "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())
    
    def read_data(
        self,
        input_path: str,
        format: DataFormat,
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """Read data from various formats.
        
        Args:
            input_path: Path to input file
            format: Input format (CSV, JSON, etc.)
            schema: Optional schema for the data
            options: Additional read options
        
        Returns:
            DataFrame containing the data
        """
        try:
            reader = self.spark.read
            if schema:
                reader = reader.schema(schema)
            
            if options:
                reader = reader.options(**options)
                
            # Format-specific configurations
            if format == DataFormat.CSV:
                reader = reader.option("header", "true")
                reader = reader.option("inferSchema", "true")
            elif format == DataFormat.JSON:
                reader = reader.option("multiLine", "true")
            
            df = reader.format(format.value).load(input_path)
            logger.info(f"Successfully read {format.value} data from {input_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {format.value} data: {e}")
            raise
    
    def write_data(
        self,
        df: DataFrame,
        output_path: str,
        format: DataFormat,
        mode: str = "overwrite",
        partition_by: Optional[list] = None,
        options: Optional[Dict[str, str]] = None
    ) -> None:
        """Write data to specified format.
        
        Args:
            df: DataFrame to write
            output_path: Path to write to
            format: Output format
            mode: Write mode (overwrite, append, etc.)
            partition_by: Columns to partition by
            options: Additional write options
        """
        try:
            writer = df.write.format(format.value).mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
                
            if options:
                writer = writer.options(**options)
                
            # Format-specific configurations
            if format == DataFormat.DELTA:
                writer = writer.option("mergeSchema", "true")
            elif format == DataFormat.PARQUET:
                writer = writer.option("compression", "snappy")
            
            writer.save(output_path)
            logger.info(f"Successfully wrote {format.value} data to {output_path}")
            
        except Exception as e:
            logger.error(f"Error writing {format.value} data: {e}")
            raise
    
    def convert_format(
        self,
        input_path: str,
        output_path: str,
        input_format: DataFormat,
        output_format: DataFormat,
        schema: Optional[StructType] = None,
        partition_by: Optional[list] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> None:
        """Convert data from one format to another.
        
        Args:
            input_path: Source data path
            output_path: Destination path
            input_format: Source format
            output_format: Target format
            schema: Optional schema
            partition_by: Optional partitioning columns
            options: Additional options for read/write
        """
        try:
            # Read data
            df = self.read_data(
                input_path,
                input_format,
                schema,
                options.get('read_options') if options else None
            )
            
            # Write in new format
            self.write_data(
                df,
                output_path,
                output_format,
                partition_by=partition_by,
                options=options.get('write_options') if options else None
            )
            
            logger.info(
                f"Successfully converted {input_format.value} to "
                f"{output_format.value}"
            )
            
        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            raise
    
    def merge_delta_table(
        self,
        source_df: DataFrame,
        target_path: str,
        merge_condition: str,
        merge_actions: Dict[str, str]
    ) -> None:
        """Merge data into Delta table.
        
        Args:
            source_df: Source DataFrame
            target_path: Path to target Delta table
            merge_condition: Merge condition
            merge_actions: Dict of merge actions
        """
        try:
            if not DeltaTable.isDeltaTable(self.spark, target_path):
                source_df.write.format("delta").save(target_path)
                logger.info(f"Created new Delta table at {target_path}")
                return
                
            delta_table = DeltaTable.forPath(self.spark, target_path)
            
            # Perform merge operation
            merge_builder = delta_table.merge(
                source_df,
                merge_condition
            )
            
            # Add merge actions
            if 'whenMatched' in merge_actions:
                merge_builder = merge_builder.whenMatchedUpdate(
                    set=merge_actions['whenMatched']
                )
            if 'whenNotMatched' in merge_actions:
                merge_builder = merge_builder.whenNotMatchedInsert(
                    values=merge_actions['whenNotMatched']
                )
                
            merge_builder.execute()
            logger.info(f"Successfully merged data into {target_path}")
            
        except Exception as e:
            logger.error(f"Merge operation failed: {e}")
            raise

def main():
    """Example usage of DataConverter."""
    try:
        converter = DataConverter()
        
        # Example: Convert CSV to Parquet
        converter.convert_format(
            input_path="data/input.csv",
            output_path="data/output.parquet",
            input_format=DataFormat.CSV,
            output_format=DataFormat.PARQUET,
            partition_by=["year", "month"]
        )
        
        # Example: Convert JSON to Delta
        converter.convert_format(
            input_path="data/input.json",
            output_path="data/output.delta",
            input_format=DataFormat.JSON,
            output_format=DataFormat.DELTA,
            options={
                'read_options': {'multiLine': 'true'},
                'write_options': {'mergeSchema': 'true'}
            }
        )
        
    except Exception as e:
        logger.error(f"Data conversion failed: {e}")
        raise

if __name__ == "__main__":
    main() 