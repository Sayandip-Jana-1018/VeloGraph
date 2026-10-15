-- title: Each station's rush hour
-- question: For the 15 busiest stations, which hour of the day sends off the most riders?
-- chart: table
WITH top_stations AS (
  SELECT start_station_id AS station_id
  FROM trips
  GROUP BY start_station_id
  ORDER BY COUNT(*) DESC
  LIMIT 15
),
by_hour AS (
  SELECT t.start_station_id AS station_id, t.start_hour AS hour, COUNT(*) AS trips
  FROM trips t
  JOIN top_stations ts ON ts.station_id = t.start_station_id
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY station_id ORDER BY trips DESC) AS rnk,
    SUM(trips) OVER (PARTITION BY station_id) AS station_trips
  FROM by_hour
)
SELECT
  s.name,
  r.hour AS busiest_hour,
  r.trips AS trips_in_that_hour,
  ROUND(100 * r.trips / r.station_trips, 1) AS share_of_station_pct
FROM ranked r
JOIN stations s ON s.station_id = r.station_id
WHERE r.rnk = 1
ORDER BY r.station_trips DESC
