--	Day of the week in 2021 and 2022 which has the lowest number of single rider trips
SELECT
  pickup_day_of_week,
  COUNT(*) AS single_rider_trips
FROM
  `ninth-iris-445112-n7.tlc_dataset.tripdata`
WHERE
  Passenger_count = 1
  AND EXTRACT(YEAR FROM pickup_datetime) IN (2021, 2022)
GROUP BY
  pickup_day_of_week
ORDER BY
  single_rider_trips ASC
LIMIT 1;
