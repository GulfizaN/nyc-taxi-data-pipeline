# Additional Features

## Weather Data Integration
- Integrated NOAA GSOD dataset publicly available on Google Cloud BigQuery
- Filtered awith taxi trip data using `weatherID` as a foreign key.
- Enhanced insights with weather attributes (e.g., precipitation, temperature).

## Excel Conversion
- Converted processed data from CSV to Excel format using a script.
- Stored Excel files in the `tlc-taxi-data-processed` bucket.
- **Performance Consideration:**
  - The conversion process for large datasets is time-consuming due to the size of xlsx files.
  - Excel has a row limit (1,048,576 rows) to mitigate which, data was split into smaller subsets (e.g., by month or half-month) before conversion.
  - Even with optimizations, processing large datasets into Excel format significantly increases runtime and resource consumption.
 
**Since the .xlsx format conversion was computationally expensive and costing a lot of time, I had to terminate the process and didn't download all the files.**
