"""Data validation script for TimeStream project."""

from pyspark.sql import SparkSession
import logging

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

def validate_data(spark: SparkSession) -> bool:
    """Validate the cleaned data."""
    try:
        # Load cleaned data
        df = spark.read.format("iceberg").load("nessie.timestream.cleaned_trips")
        
        # Basic validations
        total_count = df.count()
        valid_count = df.filter(df.passenger_count > 0).count()
        
        # Schema validation
        required_columns = ['passenger_count', 'pickup_datetime', 'dropoff_datetime']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
            
        # Data validation
        if total_count == 0:
            logger.error("No data found in the table")
            return False
            
        if valid_count != total_count:
            logger.error(f"Invalid records found: {total_count - valid_count}")
            return False
            
        logger.info("All validations passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Validation failed with error: {e}")
        return False

def main():
    """Main execution function."""
    spark = None
    try:
        spark = create_spark_session()
        is_valid = validate_data(spark)
        if not is_valid:
            raise ValueError("Data validation failed")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()