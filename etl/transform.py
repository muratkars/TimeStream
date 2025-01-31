from pyspark.sql import SparkSession
from pyiceberg.catalog import load_catalog

# Initialize Spark
spark = SparkSession.builder.appName("TimeStream ETL").getOrCreate()

# Load MinIO Iceberg Catalog
catalog = load_catalog("nessie", **{
    "uri": "http://localhost:19120/api/v1",
    "default_branch": "main"
})

# Read raw data from MinIO
df = spark.read.parquet("s3a://timestream/raw/yellow_tripdata_2023-01.parquet")

# Transform: Filter trips within NYC
df_filtered = df.filter(df.passenger_count > 0)

# Save to Iceberg table
df_filtered.write.format("iceberg").mode("overwrite").save("nessie.timestream.cleaned_trips")

print("Transformation complete.")