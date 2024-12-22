CREATE OR REPLACE TABLE `ninth-iris-445112-n7.tlc_dataset.taxi_with_weatherID` AS
SELECT
  *,
  FORMAT_DATE('%Y%m%d', DATE(pickup_datetime)) AS weatherID  -- Join key
FROM
  `ninth-iris-445112-n7.tlc_dataset.tripdata`;

-- We've used a weatherID that is uniquely identified by the date itself. This connects to the weather table.
