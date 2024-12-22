# Pipeline Design

## ETL Workflow
The pipeline follows a modular ETL (Extract, Transform, Load) workflow for scalability and manageability:

### 1. Extraction
- **Script:** `tlc_data_extraction.py`
- **Process:**
  - Downloads raw taxi trip data from the NYC TLC dataset.
  - Stores the raw data in **Google Cloud Storage (GCS)** under the `tlc-taxi-data` bucket.
- **Rationale:**
  - GCS acts as a reliable and cost-effective storage solution for large volumes of raw data.
  - Storing raw data in GCS allows for easy reprocessing if transformation logic changes.

### 2. Transformation
- **Script:** `tlc_data_transformation.py`
- **Process:**
  - **Consolidation of Yearly Data:**
    - Combines data from multiple years into a single dataset.
    - **Why?** Facilitates seamless analysis across years without needing to query multiple datasets.
  - **Combining Green and Yellow Taxi Data:**
    - Both datasets have similar structures and can be merged for a unified dataset.
    - **Why?** Enables comparative analysis of taxi types and provides a holistic view of taxi operations in NYC. Eliminates the need for joins.
  - **Formatting in Parquet and Avro:**
    - Generates intermediary files in Parquet and Avro formats.
    - **Why?** Parquet is ideal for analytics due to its columnar storage, while Avro enables serialization and compatibility with downstream systems.
  - **Defining Output Schema and Feature Engineering:**
    - Standardizes schema for consistency and defines new columns, including:
      - `pickup_hour`: Extracted from `pickup_datetime` for hourly analysis.
      - `pickup_day_of_week`: Extracted from `pickup_datetime` for weekly trend analysis.
    - **Why?** These engineered features enable deeper insights into trip patterns and support advanced analytics.

- **Rationale for PySpark and Dataproc:**
  - **PySpark:**
    - Efficient for processing and transforming large datasets.
    - Supports distributed computing, making it scalable faster than libraries like Pandas.
  - **Dataproc:**
    - Manages Spark infrastructure automatically, reducing operational overhead.
    - Allows on-demand clusters, ensuring cost-efficiency.

### 3. Loading
- **Commands:** `bq load`
- **Process:**
  - Processed data is loaded into a BigQuery table (`tripdata`) for analysis and visualization.
- **Rationale:**
  - **BigQuery:** A fully-managed data warehouse that supports real-time analysis at scale. It integrates well with Looker Studio and other analytics tools.


