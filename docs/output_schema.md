# Output Schema

## `tripdata` Table
- Contains processed NYC taxi trip data.
- 
- **Schema Definition:**
  | **Field Name**           | **Type**   | **Mode**   | **Description**                                      |
  |--------------------------|------------|------------|------------------------------------------------------|
  | VendorID                | INTEGER    | NULLABLE   | Vendor identifier.                                  |
  | pickup_datetime         | TIMESTAMP  | NULLABLE   | Trip start time.                                   |
  | dropoff_datetime        | TIMESTAMP  | NULLABLE   | Trip end time.                                     |
  | PULocationID            | INTEGER    | NULLABLE   | Pick-up location ID.                               |
  | DOLocationID            | INTEGER    | NULLABLE   | Drop-off location ID.                              |
  | Passenger_count         | FLOAT      | NULLABLE   | Number of passengers.                              |
  | Trip_distance           | FLOAT      | NULLABLE   | Distance traveled during the trip (miles).         |
  | RateCodeID              | FLOAT      | NULLABLE   | Rate code ID (1-6, NULL for invalid).              |
  | Store_and_fwd_flag      | STRING     | NULLABLE   | Store and forward flag.                            |
  | Payment_type            | FLOAT      | NULLABLE   | Payment type ID (1-6, NULL for invalid).           |
  | Fare_amount             | FLOAT      | NULLABLE   | Base fare amount.                                  |
  | Extra                   | FLOAT      | NULLABLE   | Additional charges.                                |
  | MTA_tax                 | FLOAT      | NULLABLE   | Tax amount.                                        |
  | Tip_amount              | FLOAT      | NULLABLE   | Tip amount.                                        |
  | Tolls_amount            | FLOAT      | NULLABLE   | Tolls amount.                                      |
  | Improvement_surcharge   | FLOAT      | NULLABLE   | Improvement surcharge.                             |
  | Total_amount            | FLOAT      | NULLABLE   | Total trip cost.                                   |
  | Congestion_Surcharge    | FLOAT      | NULLABLE   | Congestion surcharge amount.                       |
  | taxi_type               | STRING     | NULLABLE   | Type of taxi (yellow/green).                       |
  | pickup_hour             | INTEGER    | NULLABLE   | Hour of the day for the trip start time.           |
  | pickup_day_of_week      | INTEGER    | NULLABLE   | Day of the week for the trip start time.           |
  | weatherID               | STRING     | NULLABLE   | Foreign key linking to the weather table.          |

---

## `weather` Table
- Contains weather data for NYC stations.
- **Schema Definition:**
  | **Field Name**           | **Type**   | **Mode**   | **Description**                                     |
  |--------------------------|------------|------------|-----------------------------------------------------|
  | weatherID               | STRING     | NULLABLE   | Unique identifier based on the date.               |
  | avg_temp                | FLOAT      | NULLABLE   | Average daily temperature (°F).                    |
  | daily_precipitation     | FLOAT      | NULLABLE   | Total daily precipitation (inches).                |
  | fog                     | STRING     | NULLABLE   | Fog occurrence (1 = Yes, 0 = No).                  |
  | thunder                 | STRING     | NULLABLE   | Thunder occurrence (1 = Yes, 0 = No).              |
  | rain_drizzle            | STRING     | NULLABLE   | Rain/drizzle occurrence (1 = Yes, 0 = No).         |
  | hail                    | STRING     | NULLABLE   | Hail occurrence (1 = Yes, 0 = No).                 |
  | snow_ice_pellets        | STRING     | NULLABLE   | Snow/ice pellets occurrence (1 = Yes, 0 = No).     |

---

## `taxi_with_weatherID` Table
- Combines the processed taxi trip data with weather data for NYC.
- **Schema Notes:**
  - Inherits the schema of the `tripdata` table.
  - Adds weather attributes from the `weather` table, joined using the `weatherID` foreign key.

---

## Processed Data in Google Cloud Storage
The pipeline generates the following outputs in the GCS `processed` bucket:
1. **Parquet and Avro Files:**
   - Stored in timestamped folders for version control.
   - Example path: `gs://<bucket-name>/<timestamp>/combined_tripdata_transformed.parquet/`

2. **CSV Files:**
   - Extracted using Cloud Dataflow from BigQuery.
   - Example path: `gs://<bucket-name>/<timestamp>/csv/`

3. **Excel Files:**
   - Converted from CSV format using Python scripts.
   - Example path: `gs://<bucket-name>/<timestamp>/combined_trip_data.xlsx/`
