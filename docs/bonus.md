# Additional Features

## Weather Data Integration
- Integrated NOAA GSOD dataset which is publicly available on Google Cloud BigQuery and is updated daily. This allowed simple and easy integration wihtout requesting API keys. 
- Filtered by known NYC stations (`stn = 725030`, `wban = 14732`) to get weather data for New York and picked relevant rows to create new table called `weather`.
- Simply joining weather and tripdata would result in a lot of redundant data as the weather data would repeat for the hundreds of entrys for each day field. Instead the tables were connected using `weatherID` as a foreign key.

## Excel Conversion
- Converted processed data from CSV (which we got from the Dataflow job) to Excel format using a scriptin Cloud Shell.
- Stored Excel files in the `tlc-taxi-data-processed` bucket.
- **Performance Consideration:**
  - The conversion process for large datasets is time-consuming due to the size of xlsx files.
  - Excel has a row limit (1,048,576 rows) to mitigate which, data was split into smaller subsets (e.g., by month or half-month) before conversion.
  - Even with optimizations, processing large datasets into Excel format significantly increases runtime and resource consumption.
 
**Since the .xlsx format conversion was computationally expensive and costing a lot of time, I had to terminate the process and didn't download all the files.**
