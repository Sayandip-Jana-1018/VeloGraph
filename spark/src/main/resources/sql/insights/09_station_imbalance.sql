-- title: Where bikes pile up and where they run out
-- question: Which stations gain or lose the most bikes per day through riding alone, before any rebalancing?
-- chart: barh x=net_per_day y=name series=direction
WITH flows AS (
  SELECT station_id, SUM(departures) AS departures, SUM(arrivals) AS arrivals, COUNT(DISTINCT date) AS days
  FROM station_hour
  GROUP BY station_id
),
ranked AS (
  SELECT
    f.*,
    ROUND((arrivals - departures) / days, 1) AS net_per_day,
    RANK() OVER (ORDER BY (arrivals - departures) / days) AS drain_rank,
    RANK() OVER (ORDER BY (arrivals - departures) / days DESC) AS fill_rank
  FROM flows f
  WHERE departures + arrivals >= 1000
)
SELECT
  s.name,
  CASE WHEN r.drain_rank <= 10 THEN 'drains (needs bikes)' ELSE 'fills (needs free docks)' END AS direction,
  r.net_per_day,
  r.departures,
  r.arrivals
FROM ranked r
JOIN stations s ON s.station_id = r.station_id
WHERE r.drain_rank <= 10 OR r.fill_rank <= 10
ORDER BY r.net_per_day
