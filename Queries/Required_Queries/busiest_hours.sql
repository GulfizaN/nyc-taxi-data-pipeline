--	The top 3 of the busiest hours
SELECT
  pickup_hour,
  COUNT(*) AS trip_count
FROM
  `ninth-iris-445112-n7.tlc_dataset.tripdata`
GROUP BY
  pickup_hour
ORDER BY
  trip_count DESC
LIMIT 3;
