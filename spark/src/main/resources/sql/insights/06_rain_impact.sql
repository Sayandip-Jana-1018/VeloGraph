-- title: What rain does to riding
-- question: On working days from 07:00 to 20:59, how many trips start per hour when it is dry, drizzling, raining or pouring?
-- chart: bar x=rain y=avg_trips_per_hour
WITH hourly AS (
  -- System-wide departures per hour, daytime working hours only, so night and weekend hours do not skew it.
  SELECT ts_hour, SUM(departures) AS trips
  FROM station_hour
  WHERE NOT is_weekend AND NOT is_holiday AND hour BETWEEN 7 AND 20
  GROUP BY ts_hour
),
bucketed AS (
  SELECT
    CASE WHEN w.precip_mm < 0.1 THEN 0 WHEN w.precip_mm < 1 THEN 1 WHEN w.precip_mm < 4 THEN 2 ELSE 3 END AS bucket,
    h.trips
  FROM hourly h
  JOIN weather w ON w.ts_hour = h.ts_hour
)
SELECT
  CASE bucket
    WHEN 0 THEN 'dry'
    WHEN 1 THEN 'light (< 1 mm/h)'
    WHEN 2 THEN 'moderate (1-4 mm/h)'
    ELSE 'heavy (4+ mm/h)'
  END AS rain,
  COUNT(*) AS hours,
  ROUND(AVG(trips), 0) AS avg_trips_per_hour,
  ROUND(100 * AVG(trips) / FIRST_VALUE(AVG(trips)) OVER (ORDER BY bucket) - 100, 1) AS vs_dry_pct
FROM bucketed
GROUP BY bucket
ORDER BY bucket
