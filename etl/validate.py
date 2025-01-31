from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TimeStream Validation").getOrCreate()

# Load cleaned data
df = spark.read.format("iceberg").load("nessie.timestream.cleaned_trips")

# Run validations
valid_data = df.filter(df.passenger_count > 0)

# Count validation
if df.count() == valid_data.count():
    print("Validation passed.")
else:
    print("Validation failed.")