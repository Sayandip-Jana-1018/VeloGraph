-- title: How long is a ride?
-- question: How are trip durations distributed in 5-minute bins, with everything from 60 minutes up grouped together?
-- chart: bar x=minutes_from y=trips
WITH binned AS (
  SELECT LEAST(CAST(FLOOR(duration_s / 300) * 5 AS INT), 60) AS minutes_from, COUNT(*) AS trips
  FROM trips
  GROUP BY 1
)
SELECT
  minutes_from,
  trips,
  ROUND(100 * trips / SUM(trips) OVER (), 2) AS share_pct,
  ROUND(100 * SUM(trips) OVER (ORDER BY minutes_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    / SUM(trips) OVER (), 1) AS cumulative_pct
FROM binned
ORDER BY minutes_from
