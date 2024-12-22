-- The average distance driven by yellow and green taxis per hour
SELECT
  taxi_type,
  pickup_hour,
  AVG(Trip_distance) AS avg_distance
FROM
  `ninth-iris-445112-n7.tlc_dataset.tripdata`
GROUP BY
  taxi_type,
  pickup_hour
ORDER BY
  taxi_type,
  pickup_hour;
