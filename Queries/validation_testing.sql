-- To check the integrity of the dataset table
SELECT *
FROM `ninth-iris-445112-n7.tlc_dataset.tripdata`
WHERE 
    (Fare_amount < 0 OR MTA_tax < 0 OR Tolls_amount < 0 OR Tip_amount < 0 OR Total_amount < 0)
    OR
    (RateCodeID NOT BETWEEN 1 AND 6)
    OR
    (Trip_distance <= 0)
    OR
    (Trip_distance > 0 AND Total_amount = 0);
