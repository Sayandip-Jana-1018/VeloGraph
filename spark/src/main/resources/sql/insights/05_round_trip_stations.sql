-- title: Where people ride for fun
-- question: Which busy stations have the highest share of round trips, rides that end where they started?
-- chart: barh x=round_trip_pct y=name
SELECT
  s.name,
  COUNT(*) AS departures,
  ROUND(100 * AVG(CASE WHEN t.is_round_trip THEN 1 ELSE 0 END), 1) AS round_trip_pct
FROM trips t
JOIN stations s ON s.station_id = t.start_station_id
GROUP BY s.name
HAVING COUNT(*) >= 2000
ORDER BY round_trip_pct DESC
LIMIT 15
