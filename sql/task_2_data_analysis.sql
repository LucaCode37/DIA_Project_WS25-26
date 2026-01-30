-- Task 2.1
-- Input: a station name (here I use an example alexanderplatz).
-- Output: its coordinates and identifier (EVA).

SELECT station_name, lat, lon, eva AS identifier
FROM dim_station
WHERE station_name ILIKE '%' || 'alexanderplatz' || '%'
ORDER BY station_name
LIMIT 1;


-- Task 2.2
-- Input: a latitude/longitude (here I use an example location near Berlin city center).
-- Output: the name of the closest station.
-- Idea: I use a simple squared-distance in (lat, lon) space.

WITH input(lat, lon) AS (
  VALUES (52.5200, 13.4050)
)
SELECT s.station_name
FROM dim_station s
CROSS JOIN input i
WHERE s.lat IS NOT NULL AND s.lon IS NOT NULL
ORDER BY (s.lat - i.lat)^2 + (s.lon - i.lon)^2
LIMIT 1;



-- Task 2.3
-- Input: snapshot folder name in YYMMDDHHMM (example: 2509021800).
-- Output: total number of cancelled trains over all stations at that snapshot.
-- Idea: I count DISTINCT (station_key, stop_id) to avoid double counting the same stop
-- and if both arrival (A) and departure (D) are flagged as cancelled.

WITH params AS (
  SELECT ('20' || '2509021800')::bigint AS snapshot_time_key
)
SELECT COUNT(DISTINCT (f.station_key, f.stop_id)) AS cancelled_trains
FROM fact_train_movement f
JOIN params p ON f.snapshot_time_key = p.snapshot_time_key
WHERE f.is_cancelled = TRUE;


-- Task 2.4
-- Input: a station name (I use ILIKE to be robust to prefixes like "Berlin ...").
-- Output: average delay (minutes) at that station.
-- Idea: I use departures only (event_type='D') to avoid counting the same train twice. I also exclude cancelled events and NULL delays.

SELECT
  s.station_name,
  ROUND(AVG(f.delay_minutes)::numeric, 2) AS avg_delay_minutes,
  COUNT(*) AS num_events_used
FROM dim_station s
JOIN fact_train_movement f
  ON f.station_key = s.station_key
WHERE s.station_name ILIKE '%' || 'alexanderplatz' || '%'
  AND f.event_type = 'D'
  AND f.is_cancelled = FALSE
  AND f.delay_minutes IS NOT NULL
GROUP BY s.station_name
ORDER BY num_events_used DESC;