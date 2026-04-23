SELECT
    city,
    date_trunc('week', date) AS week_start,
    SUM(precipitation) AS weekly_precipitation
FROM USER_DB_BOA.raw.weather_data_lab1
WHERE city IN ('San Jose', 'Lake Tahoe')
GROUP BY 
    city,
    week_start
ORDER BY
    week_start