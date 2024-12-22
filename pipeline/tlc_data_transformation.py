from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, hour, dayofweek, abs, when
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType
)
from pyspark.sql.utils import AnalysisException
from datetime import datetime

def get_cleaned_dataframe(file_path, taxi_type, spark):
    try:
        print(f"Processing file: {file_path}")

        # Read the Parquet file
        df = spark.read.format("parquet").load(file_path)

        # Rename columns for consistency
        pickup_col = "tpep_pickup_datetime" if taxi_type == "yellow" else "lpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime" if taxi_type == "yellow" else "lpep_dropoff_datetime"

        if pickup_col not in df.columns or dropoff_col not in df.columns:
            print(f"Missing pickup or dropoff columns in {file_path}. Skipping...")
            return None

        df = df.withColumnRenamed(pickup_col, "pickup_datetime") \
               .withColumnRenamed(dropoff_col, "dropoff_datetime") \
               .withColumn("taxi_type", lit(taxi_type))

        return df
    except AnalysisException as ae:
        print(f"File {file_path} not found or unreadable: {ae}")
        return None
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# Initialize Spark session
spark = SparkSession.builder.appName("TLC Data Transformation").getOrCreate()

# Define schema (common columns only)
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("Passenger_count", FloatType(), True),
    StructField("Trip_distance", FloatType(), True),
    StructField("RateCodeID", FloatType(), True),
    StructField("Store_and_fwd_flag", StringType(), True),
    StructField("Payment_type", FloatType(), True),
    StructField("Fare_amount", FloatType(), True),
    StructField("Extra", FloatType(), True),
    StructField("MTA_tax", FloatType(), True),
    StructField("Tip_amount", FloatType(), True),
    StructField("Tolls_amount", FloatType(), True),
    StructField("Improvement_surcharge", FloatType(), True),
    StructField("Total_amount", FloatType(), True),
    StructField("Congestion_Surcharge", FloatType(), True),
    StructField("taxi_type", StringType(), True)
])

# Input and output paths
input_bucket = "gs://tlc-taxi-data/"
output_bucket = "gs://tlc-taxi-data-processed/"

# Generate timestamped folder
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_path_parquet = f"{output_bucket}{timestamp}/combined_tripdata_transformed.parquet"
output_path_avro = f"{output_bucket}{timestamp}/combined_tripdata_transformed.avro"

# Taxi types and years
taxi_types = ["yellow", "green"]
#current_year = datetime.now().year
#start_year = current_year - 2
current_year = 2023
start_year = 2021

# Process and combine datasets for each taxi type
yellow_combined = None
green_combined = None
for year in range(start_year, current_year + 1):
    for month in range(1, 13):
        file_path_yellow = f"{input_bucket}yellow_tripdata_{year}-{month:02d}.parquet"
        file_path_green = f"{input_bucket}green_tripdata_{year}-{month:02d}.parquet"

        yellow_df = get_cleaned_dataframe(file_path_yellow, "yellow", spark)
        green_df = get_cleaned_dataframe(file_path_green, "green", spark)

        if yellow_df:
            yellow_combined = yellow_df if yellow_combined is None else yellow_combined.union(yellow_df)
        if green_df:
            green_combined = green_df if green_combined is None else green_combined.union(green_df)

# Align schemas and combine green and yellow data
columns_to_select = [
    "VendorID", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID",
    "Passenger_count", "Trip_distance", "RateCodeID", "Store_and_fwd_flag", "Payment_type",
    "Fare_amount", "Extra", "MTA_tax", "Tip_amount", "Tolls_amount", "Improvement_surcharge",
    "Total_amount", "Congestion_Surcharge", "taxi_type"
]

if green_combined:
    green_combined = green_combined.select(columns_to_select)

if yellow_combined:
    yellow_combined = yellow_combined.select(columns_to_select)

combined_data = green_combined.union(yellow_combined)

# Data cleaning
# Drop rows where Trip_distance = 0 OR Trip_distance > 0 AND Total_amount = 0 (Since this is only 1.6% of the dataset)
combined_data = combined_data.filter(
    (col("Trip_distance") > 0) & ~((col("Trip_distance") > 0) & (col("Total_amount") == 0))
)

# Replace negative values with absolute equivalents
columns_to_clean = [
    "Fare_amount", "Tip_amount", "MTA_tax", "Tolls_amount",
    "Congestion_Surcharge", "Improvement_surcharge", "Total_amount"
]
for col_name in columns_to_clean:
    combined_data = combined_data.withColumn(col_name, abs(col(col_name)))

# Replace invalid RateCodeID and Payment_type with NULL
combined_data = combined_data.withColumn(
    "RateCodeID", when((col("RateCodeID") < 1) | (col("RateCodeID") > 6), None).otherwise(col("RateCodeID"))
)
combined_data = combined_data.withColumn(
    "Payment_type", when((col("Payment_type") < 1) | (col("Payment_type") > 6), None).otherwise(col("Payment_type"))
)

# Add time-based features
combined_data = combined_data \
    .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
    .withColumn("pickup_day_of_week", dayofweek(col("pickup_datetime")))

# Write final output if data exists
if combined_data:
    try:
        # Save final combined Parquet file
        combined_data.write.mode("overwrite").parquet(output_path_parquet)
        
        # Save final combined Avro file
        combined_data.write.mode("overwrite").format("avro").save(output_path_avro)
        
        print(f"Data successfully written to Parquet and Avro at {output_bucket}{timestamp}/")
    except Exception as e:
        print(f"Error writing output data: {e}")
else:
    print("No valid data processed.")

# Stop Spark session
spark.stop()
