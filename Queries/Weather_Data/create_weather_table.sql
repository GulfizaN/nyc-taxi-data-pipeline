CREATE OR REPLACE TABLE `ninth-iris-445112-n7.tlc_dataset.weather` AS
SELECT
  FORMAT_DATE('%Y%m%d', date) AS weatherID,  -- Unique identifier for each date
  AVG(temp) AS avg_temp,
  SUM(prcp) AS daily_precipitation,
  MAX(fog) AS fog,
  MAX(thunder) AS thunder,
  MAX(rain_drizzle) AS rain_drizzle,
  MAX(hail) AS hail,
  MAX(snow_ice_pellets) AS snow_ice_pellets
FROM
  (
    -- Filtering using some of the known weather station numbers in NY
    SELECT * FROM `bigquery-public-data.noaa_gsod.gsod2021` 
    WHERE (stn = '725030' AND wban = '14732') OR (stn = '725053' AND wban = '94728')
    UNION ALL
    SELECT * FROM `bigquery-public-data.noaa_gsod.gsod2022` 
    WHERE (stn = '725030' AND wban = '14732') OR (stn = '725053' AND wban = '94728')
    UNION ALL
    SELECT * FROM `bigquery-public-data.noaa_gsod.gsod2023` 
    WHERE (stn = '725030' AND wban = '14732') OR (stn = '725053' AND wban = '94728')
  )
GROUP BY
  date
ORDER BY
  date;
