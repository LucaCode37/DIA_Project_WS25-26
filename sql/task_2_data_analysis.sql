--Task 2.1 with "Alexanderplatz" as example

SELECT station_name, eva, latitude, longitude
FROM dim_station
WHERE station_name ILIKE '%Berlin Alexanerplatz%'


--Task 2.2

SELECT station_name, eva, latitude, longitude, SQRT(POWER(latitude - 52.5219, 2) + POWER(longitude - 13.4132, 2)) AS distance
FROM dim_station
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
ORDER BY distance ASC
LIMIT 1;


--Task 2.3

SELECT COUNT(*) AS canceld
FROM fact_train_movement f
JOIN dim_snapshot s ON s.snapshot_id = f.snapshot_id
WHERE f.is_canceled = TRUE
    AND s.snapshot_ts >= '2025-09-02 16:00:00'
    AND s.snapshot_ts < ('2025-09-02 16:00:00' + INTERVAL '1 hour')
    AND s.granularity in ('MIN15', 'HOUR');


--Task 2.4

SELECT s.station_name, AVG(d.delay_min) AS avg_delay_min
FROM dim_station s 
JOIN fact_train_movement f ON f.station_id = s.station_id
CROSS JOIN LATERAL (
    SELECT unnest(ARRAY[f.arrival_delay_min, f.departure_delay_min]) AS delay_min
) d
WHERE s.station_name ILIKE '%Berlin Alexanerplatz%'
    AND d.delay_min IS NOT NULL
    AND f.is_canceled = FALSE
GROUP BY s.station_name;



