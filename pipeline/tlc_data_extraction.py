from google.cloud import storage
import requests
from datetime import datetime

def download_and_upload_to_gcs(year, month, taxi_type, bucket_name):
    # Construct the file URL
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    file_url = f"{base_url}/{file_name}"

     # Check if the file already exists in the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if blob.exists():
        print(f"{file_name} already exists in the bucket. Skipping...")
        return

    # Download the file
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
        local_file_path = f"./{file_name}"
        with open(local_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)

        # Upload to GCS
        blob.upload_from_filename(local_file_path)
        print(f"Uploaded {file_name} to GCS bucket {bucket_name}")
    else:
        print(f"Failed to download {file_url}")

# Iterate through years, months, and taxi types
def process_all_data(start_year, end_year, bucket_name):
    taxi_types = ['yellow', 'green']
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for taxi_type in taxi_types:
                print(f"Processing {taxi_type} data for {year}-{month:02d}...")
                download_and_upload_to_gcs(year, month, taxi_type, bucket_name)

# Dynamically calculate year range 
current_year = datetime.now().year 
start_year = current_year - 2

process_all_data(start_year, current_year, 'tlc-taxi-data')

# NOTE: Since the data for 2024 wasn't fully available, I resorted to the year range 2021-2023
