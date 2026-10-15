-- title: Riding weather
-- question: In dry daytime hours on working days, how does demand change with temperature?
-- chart: line x=temp_band_c y=avg_trips_per_hour
WITH hourly AS (
  SELECT ts_hour, SUM(departures) AS trips
  FROM station_hour
  WHERE NOT is_weekend AND NOT is_holiday AND hour BETWEEN 7 AND 20
  GROUP BY ts_hour
)
SELECT
  CAST(FLOOR(w.temp_c / 3) * 3 AS INT) AS temp_band_c,
  COUNT(*) AS hours,
  ROUND(AVG(h.trips), 0) AS avg_trips_per_hour
FROM hourly h
JOIN weather w ON w.ts_hour = h.ts_hour
WHERE w.precip_mm < 0.1 -- dry hours only, so rain does not blur the temperature effect
GROUP BY 1
HAVING COUNT(*) >= 10
ORDER BY temp_band_c
