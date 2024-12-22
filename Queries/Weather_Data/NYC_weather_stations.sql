-- Finding NY weather stations to filter the dataset for NY's weather
-- Note: This isn't the full list of NY weather stations and neither does this dataset contain all of them. Due to time constraints I've resorted to 3 of NY's weather stations
SELECT DISTINCT
  stn AS station_number,
  wban
FROM
  `bigquery-public-data.noaa_gsod.gsod2021` 
WHERE
  stn IN ('725030', '725033', '725053')  -- Examples: JFK Airport, Central Park, LaGuardia
ORDER BY
  station_number;

