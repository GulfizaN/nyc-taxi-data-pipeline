# NYC Taxi Data ETL Pipeline with Weather Integration

## Table of Contents
1. [Overview](#overview)
2. [Pipeline Orchestration](#pipeline-orchestration)
3. [Setup Instructions](#setup-instructions)
   - [Prerequisites](#prerequisites)
   - [Steps](#steps)
4. [Run the Pipeline](#run-the-pipeline)
   - [Manual Execution](#manual-execution)
   - [Automated Orchestration](#automated-orchestration-future-proofing)
5. [Outputs](#outputs)
   - [Core Pipeline Outputs](#core-pipeline-outputs)
   - [Additional Outputs](#additional-outputs)
6. [Resources & Links](#resources--links)

---

## Overview
This project implements a scalable ETL pipeline to process NYC Taxi trip data for insights and analytics. The pipeline automates the following steps:
1. **Extraction:** Downloads raw taxi data from the NYC TLC dataset.
2. **Transformation:** Cleans, validates, and formats the data into Parquet and Avro formats for efficient storage.
3. **Loading:** Loads the processed data into BigQuery for analysis and visualization.

---

### Pipeline Orchestration
The pipeline is orchestrated using **Cloud Composer** (based on Apache Airflow). Tasks are scheduled and monitored to ensure reliable execution, including:
- Triggering the extraction, transformation, and loading steps.
- Managing dependencies between tasks.
- Automatically processing newly available datasets in the NYC TLC dataset.

Additionally, the project includes optional bonus features:
- **Weather Data Integration:** Enhances the dataset by joining weather attributes for advanced predictions.
- **Excel Conversion:** Converts processed data into Excel format for compatibility with tools like Microsoft Excel.

---

## Setup Instructions

### Prerequisites
1. **Google Cloud Platform (GCP)** with the following services enabled:
   - **BigQuery** (for processed data)
   - **Cloud Storage** (for raw data)
   - **Cloud Datapro** (for processing, alternatively can be done locally with sdk and connectors to cut down costs)
   - **Cloud Composer** (or alternative orchestration tools like Cloud Scheduler)
   - **Dataflow** (optional for large-scale processing)
2. **Python 3.7+** with dependencies installed (`pandas`, `google-cloud` modules).
3. Access to the NYC TLC dataset and the NOAA weather data (or Google Cloud's GSOD dataset).

### Steps
1. **Clone the repository.**
   ```bash
   git clone https://github.com/GulfizaN/nyc-taxi-data-pipeline.git
   cd nyc-taxi-data-pipeline

### Set up GCP project.

1. Create buckets for raw and processed data. 
2. Replace placeholders like `project_id` and `input_bucket` in configuration files.
3. Ensure the necessary IAM roles and permissions are granted to service accounts.

---

## Run the pipeline
### Manual Execution
1. **Extract Data:**  
   Run the extraction script:
   ```python
   python pipeline/tlc_data_extraction.py

2. **Transform Data:**  
   Submit the transformation job to Dataproc
 ```bash
   gcloud dataproc jobs submit pyspark tlc_data_transformation.py \
       --cluster=your-cluster-name \
       --region=your-region \
       --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar \
  ```
   Ensure the `spark.jars.packages property` is set for Avro transformations.

3. **Load Data to Bigquery:**  
   Use `bq load` commands to load the processed data.

### Automated Orchestration (Future-proofing)
The DAG handles all the above steps, including:
- Running the extraction script.
- Triggering Dataproc for transformations.
- Loading processed data into BigQuery.
To deploy the DAG, upload it to your Cloud Composer environment and upload the scripts to a new bucket for Composer to access them.

---

## Outputs
The pipeline generates the following outputs:

### Core Pipeline Outputs
1. **Processed Data in GCS:**
   - Parquet and Avro formats stored in timestamped folders for version controlling.
   - Example path: `gs://<bucket-name>/<timestamp>/combined_tripdata_transformed.parquet/`

   <img width="262" alt="bucket2" src="https://github.com/user-attachments/assets/b45fab36-3203-47be-bc41-3c018fb320d8" />

2. **BigQuery Tables:**
   - Processed data is stored in the `tripdata` table in the dataset.
   - Example: `project_id:dataset_id.tripdata`

3. **Looker Studio Report:**
   - Visualizations include average trip distance by hour, busiest days, and more.

### Additional Outputs
- Weather-enhanced trip data is stored in BigQuery (`taxi_with_weatherID` table which is connected to `weather` table with the weatherID foreign key).
- CSV files from BigQuery generated using Cloud Dataflow
- An additional ccript converts CSV files to Excel format.
   - Example path: `gs://<bucket-name>/<timestamp>/combined_trip_data.xlsx/`

---

## Resources & Links
- **NYC TLC Dataset**: [NYC Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **NOAA GSOD Dataset**: [Google Cloud Public Datasets](https://console.cloud.google.com/marketplace/product/noaa-public/gsod)
- **Looker Studio Dashboard**: 
