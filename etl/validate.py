"""Data validation script for TimeStream project using Great Expectations."""

import logging
from typing import Dict, List
from pathlib import Path

from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session."""
    return (SparkSession.builder
        .appName("TimeStream Validation")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate())

def create_expectation_suite() -> gx.core.ExpectationSuite:
    """Create Great Expectations suite for taxi data validation."""
    context = gx.get_context()
    
    suite_name = "taxi_data_suite"
    context.create_expectation_suite(suite_name, overwrite_existing=True)
    
    suite = context.get_expectation_suite(suite_name)
    
    # Add expectations
    expectations = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "passenger_count"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "passenger_count",
                "min_value": 1,
                "max_value": 8
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "pickup_datetime"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "dropoff_datetime"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "pickup_datetime"}
        )
    ]
    
    for expectation in expectations:
        suite.add_expectation(expectation)
    
    context.save_expectation_suite(suite)
    return suite

def validate_with_great_expectations(spark: SparkSession, table_name: str) -> bool:
    """Validate data using Great Expectations."""
    try:
        # Initialize GX context
        context = gx.get_context()
        
        # Load data
        df = spark.read.format("iceberg").load(table_name)
        
        # Create suite if not exists
        suite = create_expectation_suite()
        
        # Create batch request
        batch_request = RuntimeBatchRequest(
            datasource_name="my_spark_datasource",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="taxi_data",
            batch_identifiers={"default_identifier_name": "default_identifier"},
            runtime_parameters={"batch_data": df}
        )
        
        # Validate data
        checkpoint = context.add_or_update_checkpoint(
            name="taxi_data_checkpoint",
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite.expectation_suite_name
                }
            ]
        )
        
        results = checkpoint.run()
        
        # Generate validation report
        report_path = Path("validation_results")
        report_path.mkdir(exist_ok=True)
        
        context.build_data_docs()
        
        # Check if validation passed
        success = results["success"]
        
        if success:
            logger.info("Data validation passed all expectations!")
        else:
            logger.error("Data validation failed expectations!")
            logger.error(f"See detailed report in {report_path}")
            
        return success
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}")
        return False

def main():
    """Main execution function."""
    spark = None
    try:
        spark = create_spark_session()
        is_valid = validate_with_great_expectations(
            spark=spark,
            table_name="nessie.timestream.cleaned_trips"
        )
        if not is_valid:
            raise ValueError("Data validation failed")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()