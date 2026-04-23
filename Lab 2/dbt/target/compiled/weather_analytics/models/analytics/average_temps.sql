SELECT city, avg(temp_max) AS average_max_temp, avg(temp_min) AS average_min_temp
FROM USER_DB_BOA.analytics.stg_weather_data
GROUP BY city