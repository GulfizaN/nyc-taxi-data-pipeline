# This is just to check and validate the schema of the transformed dataset, it isn't part of the automated pipeline

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Parquet Schema Validation").getOrCreate()

# Load the combined Parquet file
parquet_file_path = "/home/gulfizanaseem/combined_tripdata_transformed.parquet"  
df = spark.read.parquet(parquet_file_path)

# Print schema
print("Schema of Combined Dataset:")
df.printSchema()

# Show first few rows
print("Sample Data:")
df.show(20)

# Stop Spark session
spark.stop()
