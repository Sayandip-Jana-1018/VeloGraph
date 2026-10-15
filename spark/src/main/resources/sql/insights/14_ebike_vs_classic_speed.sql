-- title: Are e-bikes faster?
-- question: How do speed, distance and duration compare between e-bikes and classic bikes, for members and casual riders?
-- chart: bar x=rideable_type y=median_kmh series=member_casual
SELECT
  rideable_type,
  member_casual,
  COUNT(*) AS trips,
  ROUND(percentile_approx(speed_kmh, 0.5), 1) AS median_kmh,
  ROUND(percentile_approx(distance_m, 0.5) / 1000, 2) AS median_km,
  ROUND(percentile_approx(duration_s, 0.5) / 60, 1) AS median_minutes
FROM trips
WHERE NOT is_round_trip AND duration_s >= 120 -- straight-line speed is meaningless for round trips
GROUP BY rideable_type, member_casual
ORDER BY rideable_type, member_casual
