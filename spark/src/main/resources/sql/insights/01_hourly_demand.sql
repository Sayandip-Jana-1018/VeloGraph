-- title: When does New York ride?
-- question: How many trips start in each hour of the day, on working days versus weekends and holidays?
-- chart: line x=hour y=avg_trips series=day_type
SELECT
  t.start_hour AS hour,
  CASE WHEN c.is_weekend OR c.is_holiday THEN 'weekend or holiday' ELSE 'working day' END AS day_type,
  -- Average over the days of each type, so the two curves are comparable despite different day counts.
  ROUND(COUNT(*) / COUNT(DISTINCT t.start_date), 1) AS avg_trips
FROM trips t
JOIN calendar c ON c.date = t.start_date
GROUP BY 1, 2
ORDER BY day_type, hour
