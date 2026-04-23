
SELECT
    city,
    date_trunc('week', weather_date) AS week_start,
    SUM(precipitation_sum) AS weekly_precipitation
FROM USER_DB_BOA.analytics.stg_weather_data
WHERE city IN ('San Jose', 'Lake Tahoe')
GROUP BY 1, 2
ORDER BY 2, 1