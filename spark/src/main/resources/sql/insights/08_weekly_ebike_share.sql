-- title: The e-bike takeover
-- question: What share of trips are on e-bikes each week, and how does it move week to week?
-- chart: line x=week_start y=ebike_pct
WITH weekly AS (
  SELECT
    date_trunc('week', start_date) AS week_start,
    COUNT(*) AS trips,
    100 * AVG(CASE WHEN rideable_type = 'electric_bike' THEN 1 ELSE 0 END) AS ebike_pct
  FROM trips
  GROUP BY 1
)
SELECT
  date_format(week_start, 'yyyy-MM-dd') AS week_start,
  trips,
  ROUND(ebike_pct, 2) AS ebike_pct,
  ROUND(ebike_pct - LAG(ebike_pct) OVER (ORDER BY week_start), 2) AS change_pts,
  ROUND(AVG(ebike_pct) OVER (ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rolling_4wk_pct
FROM weekly
ORDER BY week_start
