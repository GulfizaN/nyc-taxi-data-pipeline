# This is the planned DAG for pipeline automation and wasn't actually be implemented due to cost constraints as I would've incurred high costs for the set up of Kubernetes Engine

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
from datetime import timedelta
import json
import os

# Load Configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CONFIG_PATH, "r") as config_file:
    config = json.load(config_file)

# Extract values from the configuration
INPUT_BUCKET = config["gcs"]["input_bucket"]
PROCESSED_BUCKET = config["gcs"]["processed_bucket"]
SCRIPTS_BUCKET = config["gcs"]["scripts_bucket"]  # Bucket to store scripts
PROJECT_ID = config["bigquery"]["project_id"]
DATASET = config["bigquery"]["dataset"]
TRIPDATA_TABLE = config["bigquery"]["tables"]["tripdata"]
DATAPROC_CLUSTER = config["dataproc"]["cluster_name"]
DATAPROC_REGION = config["dataproc"]["region"]
SPARK_AVRO_PROPERTY = config["dataproc"]["spark_avro_property"]

# Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["your_email@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    "tlc_pipeline",
    default_args=default_args,
    description="TLC Data Pipeline with Composer",
    schedule_interval="0 0 1 * *",  # Monthly schedule
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Step 1: Download and Run the Extraction Script
    def download_and_run_extraction_script():
        """Downloads the extraction script from GCS and runs it locally."""
        client = storage.Client()
        bucket = client.bucket(SCRIPTS_BUCKET)
        blob = bucket.blob("tlc_data_extraction.py")
        local_script_path = "/tmp/tlc_data_extraction.py"
        blob.download_to_filename(local_script_path)
        print(f"Downloaded extraction script to {local_script_path}")

        import subprocess
        subprocess.run(["python3", local_script_path])

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=download_and_run_extraction_script,
    )

    # Step 2: Transform Data (Dataproc Job)
    dataproc_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{SCRIPTS_BUCKET}/tlc_data_transformation.py",
        },
        "properties": {
            "spark.jars.packages": SPARK_AVRO_PROPERTY,
        },
    }

    transform_task = DataprocSubmitJobOperator(
        task_id="transform_data",
        project_id=PROJECT_ID,
        location=DATAPROC_REGION,
        job=dataproc_job_config,
    )

    # Step 3: Determine Most Recent Folder in Processed Bucket
    def get_latest_processed_folder():
        """Fetch the most recent folder in the processed bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(PROCESSED_BUCKET.replace("gs://", "").strip("/"))
        blobs = list(bucket.list_blobs())
        folders = {blob.name.split('/')[0] for blob in blobs if '/' in blob.name}
        latest_folder = max(folders) if folders else None
        if not latest_folder:
            raise ValueError("No folders found in the bucket.")
        return latest_folder

    def determine_latest_folder(**kwargs):
        """Set the most recent folder for the BigQuery load task."""
        return get_latest_processed_folder()

    latest_folder_task = PythonOperator(
        task_id="determine_latest_folder",
        python_callable=determine_latest_folder,
        provide_context=True,
    )

    # Step 4: Load Parquet to BigQuery
    def load_parquet_to_bq(**kwargs):
        """Load the latest processed Parquet data to BigQuery."""
        latest_folder = get_latest_processed_folder()
        print(f"Latest folder determined: {latest_folder}")

        GCSToBigQueryOperator(
            task_id="load_parquet_to_bq",
            bucket=PROCESSED_BUCKET.replace("gs://", "").strip("/"),
            source_objects=[f"{latest_folder}/combined_tripdata_transformed.parquet/*"],
            destination_project_dataset_table=f"{PROJECT_ID}:{DATASET}.{TRIPDATA_TABLE}",
            source_format="PARQUET",
            write_disposition="WRITE_APPEND",  # Append new data
        ).execute(context=kwargs)

    load_parquet_task = PythonOperator(
        task_id="load_parquet_task",
        python_callable=load_parquet_to_bq,
    )

    # Define Task Dependencies
    extract_task >> transform_task >> latest_folder_task >> load_parquet_task
