--All examples can be changed to other stations/times etc.




--Task 2.1 with "Alexanderplatz" as example / could be changed

SELECT station_name, lat, lon, eva AS identifier
FROM dim_station
WHERE station_name ILIKE '%Alexanderplatz%'
ORDER BY station_name;


--Task 2.2

SELECT station_name
FROM dim_station
WHERE lat IS NOT NULL AND lon IS NOT NULL
ORDER BY POWER(lat - 52.5218, 2) + POWER(lon - 13.4132, 2)
LIMIT 1;



--Task 2.3 with date '2025-09-02' and hour '16' as example / could be changed

SELECT COUNT(DISTINCT (station_key, stop_id)) AS cancelled_trains
FROM fact_train_movement
WHERE snapshot_time_key = ('20' || '2509021600')::bigint
AND is_cancelled = TRUE;



--Task 2.4 again with "Alexanderplatz" as example / could be changed
SELECT s.station_name,
ROUND(AVG(f.delay_minutes)::numeric, 2) AS avg_delay_minutes,
COUNT(*) AS num_events_used
FROM dim_station s
JOIN fact_train_movement f ON f.station_key = s.station_key
WHERE s.station_name ILIKE '%Alexanderplatz%'
AND f.event_type = 'D'
AND f.is_cancelled = FALSE
AND f.delay_minutes IS NOT NULL
GROUP BY s.station_name
ORDER BY s.station_name;






