-- title: Finding stations by name
-- question: How do SQL wildcards (% for any text, _ for exactly one character) and regular expressions pick out groups of stations?
-- chart: table
SELECT "name LIKE '%Park%'" AS pattern, 'contains "Park"' AS meaning, COUNT(*) AS stations, SUM(trips_out) AS departures
FROM stations WHERE in_trips AND name LIKE '%Park%'
UNION ALL
SELECT "name LIKE 'W % St%'", 'a numbered West street', COUNT(*), SUM(trips_out)
FROM stations WHERE in_trips AND name LIKE 'W % St%'
UNION ALL
SELECT "name LIKE 'E _ St%'", 'an East street with a one-digit number (_ is one character)', COUNT(*), SUM(trips_out)
FROM stations WHERE in_trips AND name LIKE 'E _ St%'
UNION ALL
SELECT "name RLIKE '^[0-9]+ Ave'", 'starts with a numbered avenue (regular expression)', COUNT(*), SUM(trips_out)
FROM stations WHERE in_trips AND name RLIKE '^[0-9]+ Ave'
UNION ALL
SELECT "name NOT LIKE '%&%'", 'not a street corner (no "&")', COUNT(*), SUM(trips_out)
FROM stations WHERE in_trips AND name NOT LIKE '%&%'
