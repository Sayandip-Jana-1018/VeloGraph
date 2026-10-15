-- title: Members and casual riders
-- question: Who rides, for how long, how far, and on which bikes?
-- chart: bar x=member_casual y=trips
SELECT
  member_casual,
  COUNT(*) AS trips,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct,
  ROUND(percentile_approx(duration_s, 0.5) / 60, 1) AS median_minutes,
  ROUND(AVG(distance_m) / 1000, 2) AS avg_km,
  ROUND(100 * AVG(CASE WHEN rideable_type = 'electric_bike' THEN 1 ELSE 0 END), 1) AS ebike_pct,
  ROUND(100 * AVG(CASE WHEN is_round_trip THEN 1 ELSE 0 END), 1) AS round_trip_pct
FROM trips
GROUP BY member_casual
ORDER BY trips DESC
