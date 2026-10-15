-- title: The busiest routes
-- question: Which station-to-station routes carry the most riders (round trips excluded)?
-- chart: table
-- The stations table is joined twice (a self-join): once for the start and once for the end of each route.
SELECT
  a.name AS from_station,
  b.name AS to_station,
  r.trips,
  r.trips_am_peak,
  r.trips_pm_peak,
  ROUND(r.median_duration_s / 60, 1) AS median_minutes,
  r.member_share
FROM routes r
JOIN stations a ON a.station_id = r.src_id
JOIN stations b ON b.station_id = r.dst_id
WHERE NOT r.is_self_loop
ORDER BY r.trips DESC
LIMIT 20
