USE USER_DB_BOA;
USE SCHEMA RAW;

SELECT * 
FROM raw.weather_data_lab1;

SELECT *
FROM analytics.forecast_data_lab1;

SELECT city, date as ds, temp_max as actual, NULL as forecast, NULL as lower_bound, NULL as upper_bound
FROM raw.weather_data_lab1
UNION
    SELECT "SERIES" AS city, ts as ds, NULL AS actual, forecast, lower_bound, upper_bound
    FROM analytics.forecast_data_lab1
    ORDER BY ds DESC;