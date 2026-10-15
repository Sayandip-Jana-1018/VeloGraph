-- title: Week-over-week ridership
-- question: How many trips per day did each week average, and how did that change from the week before?
-- chart: line x=week_start y=trips_per_day
WITH weekly AS (
  SELECT
    date_trunc('week', start_date) AS week_start,
    COUNT(*) AS trips,
    COUNT(DISTINCT start_date) AS days -- partial first and last weeks are compared per day
  FROM trips
  GROUP BY 1
)
SELECT
  date_format(week_start, 'yyyy-MM-dd') AS week_start,
  days,
  trips,
  ROUND(trips / days, 0) AS trips_per_day,
  ROUND(100 * (trips / days) / LAG(trips / days) OVER (ORDER BY week_start) - 100, 1) AS wow_change_pct
FROM weekly
ORDER BY week_start
