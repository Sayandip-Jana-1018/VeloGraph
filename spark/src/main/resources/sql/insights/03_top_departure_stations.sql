-- title: The busiest stations
-- question: Which 20 stations send off the most riders, and how many per day?
-- chart: barh x=departures y=name
WITH departures AS (
  SELECT start_station_id AS station_id, COUNT(*) AS departures
  FROM trips
  GROUP BY start_station_id
),
days AS (
  SELECT COUNT(DISTINCT start_date) AS n FROM trips
)
SELECT
  s.name,
  d.station_id,
  d.departures,
  ROUND(d.departures / days.n, 0) AS per_day,
  ROUND(100 * d.departures / SUM(d.departures) OVER (), 2) AS share_pct
FROM departures d
JOIN stations s ON s.station_id = d.station_id
CROSS JOIN days
ORDER BY d.departures DESC
LIMIT 20
